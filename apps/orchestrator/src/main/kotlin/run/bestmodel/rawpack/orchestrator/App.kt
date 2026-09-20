package run.bestmodel.rawpack.orchestrator

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import run.bestmodel.rawpack.orchestrator.db.Candidates
import run.bestmodel.rawpack.orchestrator.db.Captures
import run.bestmodel.rawpack.orchestrator.db.Events
import run.bestmodel.rawpack.orchestrator.db.Jobs
import run.bestmodel.rawpack.orchestrator.db.Schema
import run.bestmodel.rawpack.orchestrator.ingest.Ingest
import run.bestmodel.rawpack.orchestrator.model.Pipeline
import run.bestmodel.rawpack.orchestrator.runner.StageRunner
import run.bestmodel.rawpack.orchestrator.runner.Worker
import run.bestmodel.rawpack.schema.PackJson
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/** Server-side config: paths and knobs come from env, never hidden constants (spec R03). */
data class AppConfig(
    val packsDir: Path,
    val dbPath: Path,
    val pipelinePath: Path,
    val stagesRoot: Path,
    val port: Int,
    val gitSha: String?,
    val pollMs: Long,
    val version: String = "0.1.0",
) {
    companion object {
        fun fromEnv(env: Map<String, String> = System.getenv()): AppConfig = AppConfig(
            packsDir = Paths.get(env["PACKS_DIR"] ?: "packs"),
            dbPath = Paths.get(env["DB_PATH"] ?: "rawpack.db"),
            pipelinePath = Paths.get(env["PIPELINE_JSON"] ?: "pipeline.json"),
            stagesRoot = Paths.get(env["STAGES_ROOT"] ?: "stages"),
            port = env["ORCH_PORT"]?.toIntOrNull() ?: 8788,
            gitSha = env["GIT_SHA"],
            pollMs = env["WORKER_POLL_MS"]?.toLongOrNull() ?: 500L,
        )
    }
}

/** tusd `post-finish` hook payload (doc shape; confirmed against the installed version in R02's install). */
@Serializable
data class TusdHook(
    val Type: String,
    val Event: TusdEvent,
) {
    val isPostFinish: Boolean get() = Type == "post-finish"
}

@Serializable
data class TusdEvent(val Upload: TusdUpload)

@Serializable
data class TusdUpload(
    val ID: String,
    val Size: Long = 0,
    val Storage: TusdStorage? = null,
    val MetaData: Map<String, String> = emptyMap(),
)

@Serializable
data class TusdStorage(val Path: String? = null, val Type: String? = null)

@Serializable
data class AcceptedDto(val capture_id: Int, val pack_id: String)

@Serializable
data class RejectedDto(val pack_id: String?, val problems: List<String>)

@Serializable
data class HealthDto(val version: String, val git_sha: String?)

@Serializable
data class CaptureRowDto(
    val id: Int,
    val pack_id: String,
    val received_at: String,
    val verified: Boolean,
    val device_model: String,
)

@Serializable
data class JobRowDto(
    val id: Int,
    val stage: String,
    val impl: String,
    val status: String,
    val attempts: Int,
    val started_at: String?,
    val finished_at: String?,
    val notes: String?,
)

@Serializable
data class CandidateRowDto(val id: Int, val job_id: Int, val path: String, val kind: String, val sha256: String)

@Serializable
data class EventRowDto(val id: Int, val ts: String, val kind: String, val payload_json: String)

@Serializable
data class CaptureDetailDto(
    val capture: CaptureRowDto,
    val jobs: List<JobRowDto>,
    val candidates: List<CandidateRowDto>,
    @SerialName("events") val lastEvents: List<EventRowDto>,
)

fun main() {
    val cfg = AppConfig.fromEnv()
    Files.createDirectories(cfg.packsDir)
    val db = Database.connect("jdbc:sqlite:${cfg.dbPath.toAbsolutePath()}", driver = "org.sqlite.JDBC")
    Schema.create(db)
    val orphaned = Schema.recoverOrphanedJobs(db)
    if (orphaned > 0) log(null, null, "restart recovery: $orphaned orphaned running job(s) requeued/failed")

    val pipeline = Pipeline.load(cfg.pipelinePath)
    val ingest = Ingest(db, cfg.packsDir, pipeline)
    val worker = Worker(db, pipeline, StageRunner(cfg.stagesRoot, cfg.packsDir), cfg.packsDir, cfg.pollMs)

    CoroutineScope(Dispatchers.Default).launch { worker.loop() }

    embeddedServer(Netty, port = cfg.port) { module(cfg, db, ingest) }.start(wait = true)
}

fun Application.module(cfg: AppConfig, db: Database, ingest: Ingest) {
    install(ContentNegotiation) { json(PackJson.lenient) }
    routing {
        get("/health") {
            call.respond(HealthDto(cfg.version, cfg.gitSha))
        }

        post("/hooks/tus") {
            val body = call.receiveText()
            val hook = runCatching { PackJson.lenient.decodeFromString(TusdHook.serializer(), body) }
                .getOrElse {
                    call.respond(HttpStatusCode.BadRequest, RejectedDto(null, listOf("unparseable tusd hook: ${it.message}")))
                    return@post
                }
            if (!hook.isPostFinish) {
                call.respond(HttpStatusCode.OK, RejectedDto(null, listOf("ignored hook Type=${hook.Type}")))
                return@post
            }
            val uploadPath = hook.Event.Upload.Storage?.Path
            if (uploadPath.isNullOrBlank() || !Files.isRegularFile(Path.of(uploadPath))) {
                call.respond(HttpStatusCode.UnprocessableEntity, RejectedDto(null, listOf("upload file not found: $uploadPath")))
                return@post
            }
            val result = runCatching {
                ingest.handle(Path.of(uploadPath), hook.Event.Upload.MetaData["pack_id"], hook.Event.Upload.MetaData["sha256"])
            }.getOrElse {
                log(null, null, "ingest error: ${it.stackTraceToString()}")
                call.respond(HttpStatusCode.InternalServerError, RejectedDto(null, listOf("ingest error: ${it.message}")))
                return@post
            }
            when (result) {
                is Ingest.Result.Accepted -> call.respond(HttpStatusCode.Accepted, AcceptedDto(result.captureId, result.packId))
                is Ingest.Result.Rejected -> call.respond(HttpStatusCode.UnprocessableEntity, RejectedDto(result.packId, result.problems))
            }
        }

        get("/captures") {
            val rows = transaction(db) {
                Captures.selectAll().orderBy(Captures.id).map {
                    CaptureRowDto(it[Captures.id], it[Captures.packId], it[Captures.receivedAt], it[Captures.verified], it[Captures.deviceModel])
                }
            }
            call.respond(rows)
        }

        get("/captures/{id}") {
            val id = call.parameters["id"]?.toIntOrNull()
            if (id == null) {
                call.respondText("capture id must be an integer", ContentType.Text.Plain, HttpStatusCode.BadRequest)
                return@get
            }
            val detail = transaction(db) {
                val capture = Captures.selectAll().where { Captures.id eq id }.firstOrNull()
                    ?: return@transaction null
                val jobs = Jobs.selectAll().where { Jobs.captureId eq id }.orderBy(Jobs.seq).map {
                    JobRowDto(it[Jobs.id], it[Jobs.stage], it[Jobs.impl], it[Jobs.status], it[Jobs.attempts], it[Jobs.startedAt], it[Jobs.finishedAt], it[Jobs.notes])
                }
                val candidates = Candidates.selectAll().where { Candidates.captureId eq id }.map {
                    CandidateRowDto(it[Candidates.id], it[Candidates.jobId], it[Candidates.path], it[Candidates.kind], it[Candidates.sha256])
                }
                val events = Events.selectAll().where { Events.captureId eq id }
                    .orderBy(Events.id).toList().takeLast(50)
                    .map { EventRowDto(it[Events.id], it[Events.ts], it[Events.kind], it[Events.payloadJson]) }
                CaptureDetailDto(
                    CaptureRowDto(capture[Captures.id], capture[Captures.packId], capture[Captures.receivedAt], capture[Captures.verified], capture[Captures.deviceModel]),
                    jobs, candidates, events,
                )
            }
            if (detail == null) {
                call.respondText("no capture $id", ContentType.Text.Plain, HttpStatusCode.NotFound)
            } else {
                call.respond(detail)
            }
        }
    }
}
