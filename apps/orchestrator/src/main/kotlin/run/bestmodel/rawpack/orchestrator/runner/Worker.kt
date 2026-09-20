package run.bestmodel.rawpack.orchestrator.runner

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import run.bestmodel.rawpack.orchestrator.db.Candidates
import run.bestmodel.rawpack.orchestrator.db.Captures
import run.bestmodel.rawpack.orchestrator.db.EventLog
import run.bestmodel.rawpack.orchestrator.db.Jobs
import run.bestmodel.rawpack.orchestrator.db.nowIso
import run.bestmodel.rawpack.orchestrator.log
import run.bestmodel.rawpack.orchestrator.model.PipelineStage
import java.nio.file.Path

/**
 * The queue consumer: one coroutine polls the `job` table. It takes the oldest
 * `queued` job whose chain predecessor is `done`, claims it (`running`, attempts+1)
 * and executes it through [StageRunner]. One GPU: stages with gpu=true serialize on a
 * global [Mutex] (spec R03 — "Um lock de GPU").
 */
class Worker(
    private val db: Database,
    private val pipeline: List<PipelineStage>,
    private val runner: StageRunner,
    private val packsDir: Path,
    private val pollMs: Long = 500,
) {
    private val gpuMutex = Mutex()

    /** One claim+execute round. Returns true when a job ran (tests drive this directly). */
    suspend fun runOnce(): Boolean {
        val claimed = claimNext() ?: return false
        val (jobId, captureId, seq, stage, impl, packId, packDir) = claimed
        val cfg = pipeline.firstOrNull { it.stage == stage && it.impl == impl }
            ?: run {
                transaction(db) {
                    Jobs.update({ Jobs.id eq jobId }) {
                        it[Jobs.status] = "failed"
                        it[Jobs.finishedAt] = nowIso()
                        it[Jobs.notes] = "stage $stage/$impl not declared in pipeline.json"
                    }
                }
                log(packId, jobId.toLong(), "failed: not in pipeline.json")
                return true
            }
        // Chaining is a property of THIS stage (cfg.input): "previous" → its --in is
        // the out/ directory of the stage right before it in the chain.
        val previousStage = pipeline.getOrNull(seq - 1)?.stage

        val outcome = withContext(Dispatchers.IO) {
            if (cfg.gpu) gpuMutex.withLock { runner.run(packId, packDir, cfg, previousStage) }
            else runner.run(packId, packDir, cfg, previousStage)
        }
        val status = if (outcome.ok) "done" else "failed"
        transaction(db) {
            if (outcome.ok) {
                outcome.candidates.forEach { (relPath, sha) ->
                    Candidates.insert {
                        it[Candidates.captureId] = captureId
                        it[Candidates.jobId] = jobId
                        it[Candidates.path] = relPath
                        it[Candidates.kind] = "candidate"
                        it[Candidates.sha256] = sha
                    }
                }
            }
            Jobs.update({ Jobs.id eq jobId }) {
                it[Jobs.status] = status
                it[Jobs.finishedAt] = nowIso()
                it[Jobs.notes] = outcome.notes
            }
        }
        EventLog.write(
            db, captureId, packDir, "job_$status",
            """{"job_id":$jobId,"stage":"$stage","impl":"$impl","attempts":1,"timed_out":${outcome.timedOut}}""",
        )
        log(packId, jobId.toLong(), "$status: stage=$stage impl=$impl${outcome.notes?.let { n -> " — ${n.take(160)}" } ?: ""}")
        return true
    }

    suspend fun loop() {
        while (true) {
            if (!runOnce()) delay(pollMs)
        }
    }

    private data class Claimed(
        val jobId: Int, val captureId: Int, val seq: Int, val stage: String, val impl: String,
        val packId: String, val packDir: Path,
    )

    private fun claimNext(): Claimed? = transaction(db) {
        val queued = Jobs.selectAll().where { Jobs.status eq "queued" }
            .orderBy(Jobs.id).toList()
        val row = queued.firstOrNull { candidate ->
            // A job is runnable when every earlier job of the same capture is done.
            val blocked = Jobs.selectAll().where { Jobs.captureId eq candidate[Jobs.captureId] }
                .filter { it[Jobs.seq] < candidate[Jobs.seq] && it[Jobs.status] != "done" }
            blocked.isEmpty()
        } ?: return@transaction null
        Jobs.update({ Jobs.id eq row[Jobs.id] }) {
            it[Jobs.status] = "running"
            it[Jobs.attempts] = row[Jobs.attempts] + 1
            it[Jobs.startedAt] = nowIso()
        }
        val capture = Captures.selectAll().where { Captures.id eq row[Jobs.captureId] }.first()
        Claimed(
            row[Jobs.id], row[Jobs.captureId], row[Jobs.seq], row[Jobs.stage], row[Jobs.impl],
            capture[Captures.packId], Path.of(capture[Captures.dir]),
        )
    }
}
