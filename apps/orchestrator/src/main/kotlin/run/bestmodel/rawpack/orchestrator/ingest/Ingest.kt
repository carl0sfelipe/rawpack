package run.bestmodel.rawpack.orchestrator.ingest

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import run.bestmodel.rawpack.orchestrator.db.Captures
import run.bestmodel.rawpack.orchestrator.db.EventLog
import run.bestmodel.rawpack.orchestrator.db.Jobs
import run.bestmodel.rawpack.orchestrator.db.nowIso
import run.bestmodel.rawpack.orchestrator.log
import run.bestmodel.rawpack.orchestrator.model.PipelineStage
import run.bestmodel.rawpack.schema.PackVerifier
import java.io.BufferedInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/**
 * Ingest: tusd `post-finish` hook → untar under PACKS_DIR/<pack_id>/ → PackVerifier →
 * capture row + the pipeline's jobs queued in chain order (spec R03). A pack that fails
 * verification is rejected with HTTP 422 and NOTHING is queued.
 */
class Ingest(
    private val db: Database,
    private val packsDir: Path,
    private val pipeline: List<PipelineStage>,
) {
    sealed class Result {
        data class Accepted(val captureId: Int, val packId: String) : Result()
        data class Rejected(val packId: String?, val problems: List<String>) : Result()
    }

    fun handle(uploadPath: Path, packId: String?, uploadSha256: String?): Result {
        if (packId.isNullOrBlank()) {
            val problems = listOf("tusd MetaData without pack_id — nothing to ingest")
            EventLog.write(db, null, null, "pack_rejected", problems.toJson())
            log(null, null, "rejected: ${problems.first()}")
            return Result.Rejected(null, problems)
        }
        val packDir = packsDir.resolve(packId).normalize()
        if (!packDir.startsWith(packsDir.toAbsolutePath().normalize())) {
            val problems = listOf("pack_id escapes PACKS_DIR: $packId")
            EventLog.write(db, null, null, "pack_rejected", problems.toJson())
            return Result.Rejected(packId, problems)
        }

        val problems = mutableListOf<String>()
        try {
            Files.createDirectories(packDir)
            untar(uploadPath, packDir)
        } catch (e: BadTar) {
            problems += "bad tar entry '${e.entry}': ${e.reason}"
        }
        if (problems.isEmpty()) {
            val report = runCatching { PackVerifier.verify(packDir) }
                .onFailure { problems += "manifest unreadable/invalid: ${it.message}" }
                .getOrNull()
            if (report != null && !report.ok) {
                problems += report.problems.map { "${it.javaClass.simpleName}: ${it.path}" }
            }
        }
        if (problems.isNotEmpty()) {
            val payload = buildString {
                append("""{"pack_id":"$packId","storage":"$uploadPath","problems":""")
                append(problems.toJson())
                append('}')
            }
            EventLog.write(db, null, packDir, "pack_rejected", payload)
            log(packId, null, "rejected: ${problems.first()}")
            return Result.Rejected(packId, problems)
        }

        val manifest = PackVerifier.readManifest(packDir)
        val captureId = transaction(db) {
            val existing = Captures.selectAll().where { Captures.packId eq packId }.firstOrNull()
            val id = existing?.get(Captures.id) ?: Captures.insert {
                it[Captures.packId] = packId
                it[Captures.receivedAt] = nowIso()
                it[Captures.dir] = packDir.toAbsolutePath().toString()
                it[Captures.verified] = true
                it[Captures.deviceModel] = manifest.device.model
            }.get(Captures.id)
            // Queue the whole chain only the first time this pack arrives.
            val alreadyQueued = Jobs.selectAll().where { Jobs.captureId eq id }.toList().isNotEmpty()
            if (!alreadyQueued) {
                pipeline.forEachIndexed { seq, stage ->
                    Jobs.insert {
                        it[Jobs.captureId] = id
                        it[Jobs.seq] = seq
                        it[Jobs.stage] = stage.stage
                        it[Jobs.impl] = stage.impl
                        it[Jobs.status] = "queued"
                    }
                }
            }
            id
        }
        EventLog.write(
            db, captureId, packDir, "capture_accepted",
            """{"pack_id":"$packId","upload_sha256":${uploadSha256?.let { "\"$it\"" } ?: "null"},"device_model":"${manifest.device.model}"}""",
        )
        log(packId, null, "accepted: capture=$captureId jobs=${pipeline.size}")
        return Result.Accepted(captureId, packId)
    }

    /** untar with entry-path safety: any `..`, absolute path or NUL byte refuses the whole pack. */
    private fun untar(tar: Path, targetDir: Path) {
        TarArchiveInputStream(BufferedInputStream(Files.newInputStream(tar))).use { tin ->
            while (true) {
                val entry = tin.nextTarEntry ?: break
                val name = entry.name
                if (name.contains('\u0000')) throw BadTar(name, "NUL byte in entry name")
                val rel = Path.of(name)
                if (rel.isAbsolute || rel.any { it.toString() == ".." }) {
                    throw BadTar(name, "escapes the pack directory")
                }
                val target = targetDir.resolve(rel).normalize()
                if (!target.startsWith(targetDir.toAbsolutePath().normalize())) {
                    throw BadTar(name, "escapes the pack directory")
                }
                if (entry.isDirectory) {
                    Files.createDirectories(target)
                } else {
                    Files.createDirectories(target.parent)
                    Files.copy(tin, target, StandardCopyOption.REPLACE_EXISTING)
                }
            }
        }
    }

    private class BadTar(val entry: String, val reason: String) : Exception("$reason: $entry")
}

private fun List<String>.toJson(): String = joinToString(",", "[", "]") { "\"${it.replace("\\", "\\\\").replace("\"", "\\\"")}\"" }
