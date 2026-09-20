package run.bestmodel.rawpack.orchestrator.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import run.bestmodel.rawpack.schema.PackJson
import java.nio.file.Files
import java.nio.file.Path

/**
 * The processing chain, declared in pipeline.json at the repo root — timeouts and
 * chaining are configuration, never hidden constants (spec R03 rules).
 */
@Serializable
data class PipelineStage(
    val stage: String,
    val impl: String,
    /** "pack" → the pack directory; "previous" → the previous stage's out/ directory. */
    val input: String = "pack",
    @SerialName("timeout_s") val timeoutS: Long = 60,
    val gpu: Boolean = false,
    /** Optional path (repo-relative) to the stage's params.json; absent → stage defaults. */
    @SerialName("params_file") val paramsFile: String? = null,
)

object Pipeline {
    fun load(path: Path): List<PipelineStage> {
        require(Files.isRegularFile(path)) { "pipeline.json not found: $path" }
        val stages = PackJson.lenient.decodeFromString(ListSerializer(PipelineStage.serializer()), Files.readString(path))
        require(stages.isNotEmpty()) { "pipeline.json declares no stages" }
        stages.forEachIndexed { i, s ->
            require(s.input == "pack" || (i > 0)) { "stage '${s.stage}' chains input=previous but has no predecessor" }
        }
        return stages
    }
}
