# specs/ — MVP do rawpack, uma história por arquivo, um oráculo por história

Formato: o de `llms.surf` (`bin/check-spec.sh` + `bin/check-oracle.py`).
Cada spec tem cláusula anti-invenção, bloco **Dados verificados**, linha
`VERIFICACAO:` com comando semântico, seção **## Oráculo** com `- comando:`
em texto cru (sem crase — regra 46) e `- exit esperado:`, seção **## Barra**
e cláusula anti-fantasma. Todas passaram em `check-spec.sh` da v4.1.0 antes
de entrar aqui (ver `incidents/2026-09-20-dogfood-llms-surf-v4-1-0.md`).

## Ordem e dependências

```text
R00 semente: contrato do pack + verificador Kotlin + estágio de referência   DONE (oráculo verde 2026-09-20)
R01 probe de capacidades do S25 Ultra (Android)                              ── define o que o pack pode ter
R02 app de captura v0 (Kotlin, CameraX 1.5, tus)                            ── depende de R00 (schema) e R01 (modos)
R03 orquestrador: ingest tus + fila + runner de estágios (Ktor)              ── depende de R00  DONE (oráculo verde 2026-09-20)
R04 estágios reais: s1-fuse (hdrplus/MFSR) + s2-develop (darktable/vkdt)     ── depende de R00; roda na 3090
R05 revisão HITL mobile-first (Ktor + HTMX)                                  ── depende de R03
R06 publicação no Immich + XMP da receita                                    ── depende de R05
```

MVP = R00–R06: uma foto tirada no S25 Ultra vira pack na máquina da 3090,
é fundida e revelada em candidatos, o dono escolhe no próprio celular e o
resultado aparece no Immich com a receita ao lado. Nada de IA generativa no
MVP (R08), nada de Bend2 no MVP (R09) — ambos entram atrás do mesmo
contrato de estágio depois.

## Como despachar (llms.surf v4.1.0)

```bash
export ORACFIT_ROOT=~/llms.surf            # checkout do orquestrador
source "$ORACFIT_ROOT/adapters/opencode/env.sh"   # ou outro adapter
cd ~/Work/rawpack
"$ORACFIT_ROOT/bin/check-spec.sh" specs/R03-orquestrador-ingest-fila-runner.md
python3 "$ORACFIT_ROOT/bin/check-oracle.py" specs/R03-orquestrador-ingest-fila-runner.md "$PWD"
"$ORACFIT_ROOT/bin/llms-surf" run normal specs/R03-orquestrador-ingest-fila-runner.md r03
"$ORACFIT_ROOT/bin/llms-surf" status --task r03
```

Um dispatch por workdir por vez (regra 53). O oráculo de cada spec é
executado pelo preflight ANTES do modelo: tem de falhar por falta de
trabalho (exit 1, sem `command not found`), nunca por comando quebrado —
por isso todo oráculo começa com `test -x ./gradlew &&` ou `test -f <alvo> &&`.

## Toolchain (verificado nesta máquina, 2026-09-20)

- JDK 21 (OpenJDK 21.0.10), Gradle 8.14.3 via wrapper commitado, Kotlin 2.2.0,
  kotlinx-serialization-json 1.9.0, JUnit 5.11.4, networknt json-schema-validator 1.5.6 —
  todos resolvidos do Maven Central e usados no oráculo verde da R00.
- Android SDK/AGP: **não** existe nesta máquina; R01/R02 exigem a máquina do
  dono (SDK 35+, dispositivo SM-S938x com depuração USB). Seus oráculos de CI
  cobrem só testes de unidade JVM; a barra é o aparelho.
- 3090 + CUDA 12 + darktable-cli/vkdt: máquina do dono; R04 declara dry-run
  para o CI e execução real como barra.
