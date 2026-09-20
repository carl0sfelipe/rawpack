package run.bestmodel.rawpack.orchestrator.db

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant

/**
 * All orchestrator state lives in SQLite (spec R03, behavior 1): no Redis in the MVP,
 * the queue is the `job` table polled by a coroutine worker. `events.jsonl` per capture
 * under PACKS_DIR is the human-readable mirror, not the source of truth.
 */
object Captures : Table("capture") {
    val id = integer("id").autoIncrement()
    val packId = varchar("pack_id", 128).uniqueIndex()
    val receivedAt = varchar("received_at", 40)
    val dir = varchar("dir", 1024)
    val verified = bool("verified")
    val deviceModel = varchar("device_model", 128)
    override val primaryKey = PrimaryKey(id)
}

object Jobs : Table("job") {
    val id = integer("id").autoIncrement()
    val captureId = integer("capture_id") references Captures.id
    val seq = integer("seq")
    val stage = varchar("stage", 64)
    val impl = varchar("impl", 64)
    val status = varchar("status", 16) // queued | running | done | failed
    val attempts = integer("attempts").default(0)
    val startedAt = varchar("started_at", 40).nullable()
    val finishedAt = varchar("finished_at", 40).nullable()
    val notes = text("notes").nullable()
    override val primaryKey = PrimaryKey(id)
}

object Candidates : Table("candidate") {
    val id = integer("id").autoIncrement()
    val captureId = integer("capture_id") references Captures.id
    val jobId = integer("job_id") references Jobs.id
    val path = varchar("path", 1024)
    val kind = varchar("kind", 64)
    val sha256 = varchar("sha256", 64)
    override val primaryKey = PrimaryKey(id)
}

object Events : Table("event") {
    val id = integer("id").autoIncrement()
    val captureId = integer("capture_id").nullable()
    val ts = varchar("ts", 40)
    val kind = varchar("kind", 64)
    val payloadJson = text("payload_json")
    override val primaryKey = PrimaryKey(id)
}

fun nowIso(): String = Instant.now().toString()

object EventLog {
    /**
     * One event, two surfaces: the SQLite row (queryable, feeds GET /captures/{id})
     * and the append-only events.jsonl inside the capture directory (behavior 1).
     * A rejected pack has no capture row, but the untarred directory exists — the
     * file mirror is best-effort and never fails the caller.
     */
    fun write(db: Database, captureId: Int?, packDir: Path?, kind: String, payloadJson: String) {
        transaction(db) {
            Events.insert {
                it[Events.captureId] = captureId
                it[Events.ts] = nowIso()
                it[Events.kind] = kind
                it[Events.payloadJson] = payloadJson
            }
        }
        if (packDir != null) {
            runCatching {
                Files.createDirectories(packDir)
                Files.writeString(
                    packDir.resolve("events.jsonl"),
                    """{"ts":"${nowIso()}","kind":"$kind","payload":$payloadJson}""" + "\n",
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                )
            }
        }
    }
}

object Schema {
    fun create(db: Database) {
        transaction(db) {
            SchemaUtils.create(Captures, Jobs, Candidates, Events)
        }
    }

    /**
     * Behavior 2: a restart must not lose queued work. A `running` job means the
     * process died mid-run — back to `queued` with attempts+1; past 3 attempts it
     * stays `failed` with the reason recorded.
     */
    fun recoverOrphanedJobs(db: Database): Int = transaction(db) {
            val orphans = Jobs.selectAll().where { Jobs.status eq "running" }.toList()
            for (job in orphans) {
                val attempts = job[Jobs.attempts] + 1
                if (attempts > 3) {
                    Jobs.update({ Jobs.id eq job[Jobs.id] }) {
                        it[Jobs.status] = "failed"
                        it[Jobs.finishedAt] = nowIso()
                        it[Jobs.notes] = "orphaned running job on restart; attempts=$attempts > 3"
                    }
                } else {
                    Jobs.update({ Jobs.id eq job[Jobs.id] }) {
                        it[Jobs.status] = "queued"
                        it[Jobs.attempts] = attempts
                        it[Jobs.notes] = "orphaned running job on restart; requeued (attempts=$attempts)"
                    }
                }
            }
            orphans.size
    }
}
