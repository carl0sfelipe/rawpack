package run.bestmodel.rawpack.orchestrator.runner

import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SchemaValidatorsConfig
import com.networknt.schema.SpecVersion
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import run.bestmodel.rawpack.orchestrator.model.PipelineStage
import run.bestmodel.rawpack.schema.PackJson
import run.bestmodel.rawpack.schema.PackVerifier
import run.bestmodel.rawpack.schema.StageResult
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Executes one stage job per stages/CONTRACT.md: `stages/<stage>/<impl>/run --in --out [--params]`
 * as a subprocess with a timeout that kills the whole tree (`bin/with-timeout` semantics:
 * ProcessHandle.descendants() + destroy), then validates `result.json` against
 * `$defs/StageResult` of the pack schema before anyone reads a verdict out of it.
 */
class StageRunner(
    private val stagesRoot: Path,
    private val packsDir: Path,
) {
    data class RunOutcome(
        val ok: Boolean,
        val timedOut: Boolean,
        val notes: String?,
        val result: StageResult?,
        /** out/<stage>/-relative paths of outputs whose kind starts with "candidate". */
        val candidates: List<Pair<String, String>>, // (relative path, sha256)
    )

    fun run(packId: String, packDir: Path, cfg: PipelineStage, previousStage: String?): RunOutcome {
        // Absolute paths: ProcessBuilder resolves a relative program against the
        // subprocess `directory`, not the JVM cwd — a relative runScript would
        // be looked up INSIDE its own parent directory and vanish.
        val runScript = stagesRoot.resolve(cfg.stage).resolve(cfg.impl).resolve("run").toAbsolutePath().normalize()
        if (!Files.isRegularFile(runScript) || !Files.isExecutable(runScript)) {
            return fail(packDir, cfg, "stage run not found or not executable: $runScript")
        }
        val outDir = packDir.resolve("out").resolve(cfg.stage).toAbsolutePath().normalize()
        if (Files.exists(outDir)) outDir.toFile().deleteRecursively()
        Files.createDirectories(outDir)

        val inDir = (if (cfg.input == "previous" && previousStage != null) {
            packDir.resolve("out").resolve(previousStage)
        } else {
            packDir
        }).toAbsolutePath().normalize()
        if (!Files.isDirectory(inDir)) {
            return fail(packDir, cfg, "input dir missing: $inDir")
        }

        val cmd = mutableListOf(runScript.toString(), "--in", inDir.toString(), "--out", outDir.toString())
        cfg.paramsFile?.let { p ->
            val paramsPath = stagesRoot.resolve(p).toAbsolutePath().normalize()
            if (Files.isRegularFile(paramsPath)) cmd += listOf("--params", paramsPath.toString())
        }

        val process = runCatching {
            ProcessBuilder(cmd)
                .directory(runScript.parent.toFile())
                .redirectErrorStream(true)
                .start()
        }.getOrElse { return fail(packDir, cfg, "cannot start stage process: ${it.message}") }
        val output = StringBuilder()
        val reader = Thread {
            process.inputStream.bufferedReader().forEachLine { line ->
                synchronized(output) { output.appendLine(line) }
            }
        }
        reader.isDaemon = true
        reader.start()

        val finished = process.waitFor(cfg.timeoutS, TimeUnit.SECONDS)
        if (!finished) {
            killTree(process)
            reader.join(2000)
            val notes = "timed out after ${cfg.timeoutS}s (tree killed)"
            return RunOutcome(false, true, notes + tail(output), readResultQuietly(outDir), emptyList())
        }
        reader.join(2000)
        val rc = process.exitValue()

        val resultFile = outDir.resolve("result.json")
        if (!Files.isRegularFile(resultFile)) {
            return RunOutcome(false, false, "exit=$rc, result.json missing${tail(output)}", null, emptyList())
        }
        val text = Files.readString(resultFile)
        val schemaErrors = StageResultValidator.validate(text)
        if (schemaErrors.isNotEmpty()) {
            return RunOutcome(false, false, "exit=$rc, result.json invalid against \$defs/StageResult: ${schemaErrors.joinToString("; ")}${tail(output)}", null, emptyList())
        }
        val result = runCatching { PackJson.strict.decodeFromString(StageResult.serializer(), text) }
            .getOrElse { return RunOutcome(false, false, "exit=$rc, result.json does not decode: ${it.message}${tail(output)}", null, emptyList()) }

        val manifestSha = runCatching { PackVerifier.sha256Hex(inDir.resolve("manifest.json")) }.getOrNull()
        if (manifestSha == null) {
            return RunOutcome(false, false, "exit=$rc, input dir has no manifest.json to tie inputs_sha256 to${tail(output)}", result, emptyList())
        }
        if (result.inputsSha256 != manifestSha) {
            return RunOutcome(false, false, "exit=$rc, inputs_sha256 mismatch: result ties ${result.inputsSha256}, pack manifest is $manifestSha${tail(output)}", result, emptyList())
        }
        if (!result.ok) {
            return RunOutcome(false, false, "exit=$rc, stage reported ok:false${notesOf(result)}${tail(output)}", result, emptyList())
        }
        if (rc != 0) {
            return RunOutcome(false, false, "exit=$rc despite ok:true result.json${tail(output)}", result, emptyList())
        }
        val candidates = result.outputs
            .filter { it.kind.startsWith("candidate") }
            .map { "out/${cfg.stage}/${it.path}" to it.sha256 }
        return RunOutcome(true, false, result.notes, result, candidates)
    }

    /** bin/with-timeout.sh semantics: descendants first, then the process itself, forcibly if needed. */
    private fun killTree(process: Process) {
        val handle = process.toHandle()
        handle.descendants().forEach { it.destroy() }
        handle.destroy()
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            handle.descendants().forEach { it.destroyForcibly() }
            process.destroyForcibly()
            process.waitFor(5, TimeUnit.SECONDS)
        }
    }

    private fun readResultQuietly(outDir: Path): StageResult? {
        val f = outDir.resolve("result.json")
        if (!Files.isRegularFile(f)) return null
        return runCatching { PackJson.strict.decodeFromString(StageResult.serializer(), Files.readString(f)) }.getOrNull()
    }

    private fun fail(packDir: Path, cfg: PipelineStage, notes: String) =
        RunOutcome(false, false, notes, null, emptyList())

    private fun notesOf(result: StageResult) = if (result.notes != null) " — ${result.notes}" else ""

    private fun tail(output: StringBuilder, max: Int = 2000): String {
        val text = synchronized(output) { output.toString() }.trim()
        if (text.isEmpty()) return ""
        val cut = if (text.length <= max) text else "…${text.takeLast(max)}"
        return " | stderr/stdout tail: $cut"
    }
}

/** Validates a JSON document against one `$defs` entry of the pack schema (R00 test pattern, now production). */
object StageResultValidator {
    private val factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
    private val mapper = ObjectMapper()
    private val schemaRoot: JsonObject by lazy {
        Json.parseToJsonElement(PackJson.schemaText()).jsonObject
    }

    fun validate(text: String): List<String> {
        val wrapped = JsonObject(schemaRoot + ("\$ref" to JsonPrimitive("#/\$defs/StageResult")))
        val schema: JsonSchema = factory.getSchema(wrapped.toString(), SchemaValidatorsConfig.builder().build())
        return schema.validate(mapper.readTree(text)).map { it.message }
    }
}
