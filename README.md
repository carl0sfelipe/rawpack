# rawpack

> A camera that records the sensor and its context, a workstation that does the
> computing, a human who decides. Phone = orchestrator of sensors and recorder of
> context; the 3090 = compute; you = the loop.

The Galaxy S25 Ultra's camera app edits with AI before you see the photo. rawpack
does the opposite: the phone captures a **pack** (RAW burst + per-frame
`CaptureResult` + IMU + calibration + the vendor's own JPEG as a side channel),
ships it to a local machine with an RTX 3090, and the heavy processing — burst
fusion, development, later generative edits — runs there behind a **stage
contract**, producing candidates that you approve on your phone. The approved
image lands in Immich with its recipe next to it; the original pack is never
touched.

Sibling of [bestmodel.run](https://bestmodel.run) (same owner, same rules: numbers
are measured or absent, every story ships with an oracle, every failure becomes an
incident). Specs are written for and validated by [llms.surf](https://github.com/carl0sfelipe/llms.surf).

## State (2026-09-20)

| Story | What | Status |
|---|---|---|
| R00 | Pack contract (`pack.schema.json`), Kotlin models + `PackVerifier`, reference stage `s0-identity` | **done** — `./gradlew :shared:pack-schema:test` green (7 tests) |
| R01 | Capability probe app for the S25 Ultra | spec |
| R02 | Capture app v0 (CameraX 1.5 RAW+JPEG burst, IMU, tus upload) | spec |
| R03 | Orchestrator: tusd hook → verify → SQLite queue → stage runner | **done** — `./gradlew :apps:orchestrator:test` green (5 tests), 2026-09-20 |
| R04 | Real stages: `s1-fuse` (HDR+/MFSR), `s2-develop` (darktable/vkdt) | spec |
| R05 | Human-in-the-loop review, mobile-first (Ktor + HTMX) | spec |
| R06 | Publish approved candidate to Immich + XMP recipe | spec |

MVP = R00–R06. After it: R07 ZSL ring buffer + vendor side channel, R08 generative
stage via ComfyUI (SeedVR2, Qwen-Image-Edit, SAM 2), R09 Bend2 study behind the
same contract, R10 on-device mode decision, R11 preference loop (TPE).

## Layout

```text
shared/pack-schema/     JSON Schema (single source) + Kotlin models + PackVerifier   [R00]
stages/CONTRACT.md      how any language plugs in; stages/s0-identity/bash/run is the proof
fixtures/pack-minimal/  a valid pack whose frames are text placeholders (contract only)
specs/                  one story per file, one oracle per story (llms.surf format)
bench/                  same pack, same oracle, every implementation
incidents/              diary entries in the llms.surf format
apps/orchestrator/      Ktor server: tusd hook, SQLite queue, stage runner [R03]
apps/android/           separate Gradle build with the Android SDK (R01, R02)
```

## Run what exists

```bash
./gradlew :shared:pack-schema:test
stages/s0-identity/bash/run --in fixtures/pack-minimal --out /tmp/s0 && cat /tmp/s0/result.json
```

Toolchain: JDK 21, Gradle 8.14.3 (wrapper committed), Kotlin 2.2.0. Android SDK only
for `apps/android/`. GPU work only on the owner's machine.

## Dispatch a story

See `specs/README.md`. Short form:

```bash
export ORACFIT_ROOT=~/llms.surf && source "$ORACFIT_ROOT/adapters/opencode/env.sh"
"$ORACFIT_ROOT/bin/check-spec.sh" specs/R03-orquestrador-ingest-fila-runner.md
"$ORACFIT_ROOT/bin/llms-surf" run normal specs/R03-orquestrador-ingest-fila-runner.md r03
```

License: owner's decision, pending (proposed Apache-2.0 for everything here; GPL
projects — MotionCam, Open Camera, darktable — are called as processes, never linked).
