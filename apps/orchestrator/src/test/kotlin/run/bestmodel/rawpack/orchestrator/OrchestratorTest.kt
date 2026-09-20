package run.bestmodel.rawpack.orchestrator

import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import run.bestmodel.rawpack.orchestrator.db.Captures
import run.bestmodel.rawpack.orchestrator.db.Events
import run.bestmodel.rawpack.orchestrator.db.Jobs
import run.bestmodel.rawpack.orchestrator.db.Schema
import run.bestmodel.rawpack.orchestrator.ingest.Ingest
import run.bestmodel.rawpack.orchestrator.model.PipelineStage
import run.bestmodel.rawpack.orchestrator.runner.StageRunner
import run.bestmodel.rawpack.orchestrator.runner.Worker
import run.bestmodel.rawpack.schema.PackVerifier
import run.bestmodel.rawpack.schema.PackJson
import run.bestmodel.rawpack.schema.StageResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.io.path.readText

/**
 * The five R03 acceptance cases: ingest via tusd hook, worker through the real
 * s0-identity reference stage, tampered pack rejection, failing stage and
 * timeout with tree kill — all on temp SQLite + temp PACKS_DIR (spec R03).
 */
class OrchestratorTest {
    private val repoRoot: Path = generateSequence(Paths.get("").toAbsolutePath()) { it.parent }
        .first { Files.exists(it.resolve("fixtures/pack-minimal/manifest.json")) }
    private val fixture: Path = repoRoot.resolve("fixtures/pack-minimal")
    private val testResources: Path = Paths.get("src/test/resources")
    private val packId = "20260920T180411Z_SM-S938B_0001"

    @TempDir
    lateinit var tmp: Path

    private fun makeTar(tamper: Boolean = false): Path {
        val tar = tmp.resolve("$packId.tar")
        TarArchiveOutputStream(Files.newOutputStream(tar)).use { out ->
            Files.walk(fixture).sorted().forEach { src ->
                val rel = fixture.relativize(src).toString()
                if (rel.isEmpty()) return@forEach
                if (Files.isDirectory(src)) {
                    // trailing slash is what makes a tar entry a directory
                    out.putArchiveEntry(TarArchiveEntry("$rel/"))
                    out.closeArchiveEntry()
                } else {
                    val bytes =
                        if (tamper && rel == "frames/000.dng") "tampered\n".toByteArray()
                        else Files.readAllBytes(src)
                    val entry = TarArchiveEntry(rel)
                    entry.size = bytes.size.toLong()
                    out.putArchiveEntry(entry)
                    out.write(bytes)
                    out.closeArchiveEntry()
                }
            }
        }
        return tar
    }

    private fun hookPayload(tar: Path): String =
        testResources.resolve("tusd-post-finish.json").readText()
            .replace("@TAR_PATH@", tar.toString())
            .replace("\"Size\": 1337", "\"Size\": ${Files.size(tar)}")

    private fun pipelineFile(stages: List<PipelineStage>): Path {
        val f = tmp.resolve("pipeline.json")
        Files.writeString(f, PackJson.lenient.encodeToString(kotlinx.serialization.builtins.ListSerializer(PipelineStage.serializer()), stages))
        return f
    }

    private fun Database.jobs() = transaction(this) { Jobs.selectAll().toList() }
    private fun Database.events() = transaction(this) { Events.selectAll().toList() }
    private fun Database.captures() = transaction(this) { Captures.selectAll().toList() }

    private fun testEnv(
        pipeline: List<PipelineStage>,
        stagesRoot: Path,
        block: suspend ApplicationTestBuilder.(Database, Worker, Path) -> Unit,
    ) {
        val packsDir = tmp.resolve("packs")
        val dbPath = tmp.resolve("rawpack.db")
        val db = Database.connect("jdbc:sqlite:$dbPath", driver = "org.sqlite.JDBC")
        Schema.create(db)
        val ingest = Ingest(db, packsDir, pipeline)
        val worker = Worker(db, pipeline, StageRunner(stagesRoot, packsDir), packsDir)
        testApplication {
            application { module(AppConfig(packsDir, dbPath, pipelineFile(pipeline), stagesRoot, 0, null, 500), db, ingest) }
            runBlocking { block(db, worker, packsDir) }
        }
    }

    private suspend fun postHook(client: io.ktor.client.HttpClient, tar: Path): io.ktor.client.statement.HttpResponse =
        client.post("/hooks/tus") {
            contentType(ContentType.Application.Json)
            setBody(hookPayload(tar))
        }

    private val defaultPipeline = listOf(PipelineStage(stage = "s0-identity", impl = "bash", input = "pack", timeoutS = 60, gpu = false))

    @Test
    fun `1 hook with fixture tar returns 202, creates capture and queues the chain`() = testEnv(defaultPipeline, repoRoot.resolve("stages")) { db, _, _ ->
        val resp = postHook(client, makeTar())
        assertEquals(HttpStatusCode.Accepted, resp.status, resp.bodyAsText())
        assertTrue(resp.bodyAsText().contains("\"capture_id\""), resp.bodyAsText())

        val captures = db.captures()
        assertEquals(1, captures.size)
        assertEquals(packId, captures[0][Captures.packId])
        assertTrue(captures[0][Captures.verified])
        assertEquals("SM-S938B", captures[0][Captures.deviceModel])

        val jobs = db.jobs()
        assertEquals(1, jobs.size)
        assertEquals("queued", jobs[0][Jobs.status])
        assertEquals("s0-identity", jobs[0][Jobs.stage])
        assertEquals("bash", jobs[0][Jobs.impl])
    }

    @Test
    fun `2 worker runs s0-identity and the result ties to the pack manifest`() = testEnv(defaultPipeline, repoRoot.resolve("stages")) { db, worker, packsDir ->
        postHook(client, makeTar())
        assertTrue(worker.runOnce())

        val jobs = db.jobs()
        assertEquals(1, jobs.size)
        assertEquals("done", jobs[0][Jobs.status], "job notes: ${jobs[0][Jobs.notes]}")

        val resultFile = packsDir.resolve(packId).resolve("out/s0-identity/result.json")
        assertTrue(Files.isRegularFile(resultFile), "result.json missing")
        val result = PackJson.strict.decodeFromString(StageResult.serializer(), resultFile.readText())
        assertTrue(result.ok)
        assertEquals("s0-identity", result.stage)
        assertEquals(PackVerifier.sha256Hex(packsDir.resolve(packId).resolve("manifest.json")), result.inputsSha256)
    }

    @Test
    fun `3 tampered frame is rejected with 422, zero jobs and a pack_rejected event naming the path`() = testEnv(defaultPipeline, repoRoot.resolve("stages")) { db, _, _ ->
        val resp = postHook(client, makeTar(tamper = true))
        assertEquals(HttpStatusCode.UnprocessableEntity, resp.status)
        assertTrue(resp.bodyAsText().contains("frames/000.dng"), resp.bodyAsText())

        assertEquals(0, db.jobs().size)
        assertEquals(0, db.captures().size)
        val rejected = db.events().filter { it[Events.kind] == "pack_rejected" }
        assertEquals(1, rejected.size)
        assertTrue(rejected[0][Events.payloadJson].contains("frames/000.dng"), rejected[0][Events.payloadJson])
    }

    @Test
    fun `4 stage exiting 4 fails the job with truncated stderr in notes`() = testEnv(
        listOf(PipelineStage(stage = "fail-exit4", impl = "bash", timeoutS = 10)),
        testResources.resolve("stages"),
    ) { db, worker, _ ->
        postHook(client, makeTar())
        assertTrue(worker.runOnce())

        val jobs = db.jobs()
        assertEquals(1, jobs.size)
        assertEquals("failed", jobs[0][Jobs.status])
        val notes = jobs[0][Jobs.notes].orEmpty()
        assertTrue(notes.contains("boom exit four"), "notes should carry the stderr marker: $notes")
        assertTrue(notes.contains("exit=4"), "notes should carry the exit code: $notes")
    }

    @Test
    fun `5 stage sleeping past timeout_s is killed with no orphan process`() = testEnv(
        listOf(PipelineStage(stage = "slow", impl = "bash", timeoutS = 1)),
        testResources.resolve("stages"),
    ) { db, worker, packsDir ->
        postHook(client, makeTar())
        assertTrue(worker.runOnce())

        val jobs = db.jobs()
        assertEquals(1, jobs.size)
        assertEquals("failed", jobs[0][Jobs.status])
        assertTrue(jobs[0][Jobs.notes].orEmpty().contains("timed out"), jobs[0][Jobs.notes])

        val pidFile = packsDir.resolve(packId).resolve("out/slow/pid")
        assertTrue(Files.isRegularFile(pidFile), "slow stage should have written its pid")
        val pid = pidFile.readText().trim().toLong()
        val deadline = System.currentTimeMillis() + 3000
        var alive = true
        while (System.currentTimeMillis() < deadline) {
            alive = java.util.concurrent.CompletableFuture.supplyAsync {
                ProcessHandle.of(pid).map { it.isAlive }.orElse(false)
            }.join()
            if (!alive) break
            Thread.sleep(100)
        }
        assertTrue(!alive, "stage process $pid must not survive the timeout")
        assertTrue(!Files.exists(packsDir.resolve(packId).resolve("out/slow/late.txt")), "killed stage must not keep writing")
    }
}
