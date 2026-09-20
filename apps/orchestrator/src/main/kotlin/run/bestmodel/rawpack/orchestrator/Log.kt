package run.bestmodel.rawpack.orchestrator

import java.time.Instant

/**
 * Behavior 4: every log line on stdout carries pack_id and job_id (or '-' when the
 * line does not belong to one yet). No structured-logging dependency in the MVP.
 */
fun log(packId: String?, jobId: Long?, msg: String) {
    println("[${Instant.now()}] pack=${packId ?: "-"} job=${jobId ?: "-"} $msg")
}
