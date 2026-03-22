# Autonomous Loop Guardrails

## Goal
Define safe, high-autonomy operating rules for the Ralph-style build loop.

## Loop Startup Procedure (every iteration)

1. **Run `python scripts/sync_state.py`**
   This derives ground truth from git history and the implementation plan.
   Read its stdout to determine: current phase, next task, gate status.
   Do NOT trust any cached state — sync_state.py IS the source of truth.

2. **Read `IMPLEMENTATION_PLAN.md`**
   Get full context on the current task, its gate, and dependencies.

3. **Check the gate of the current task BEFORE starting work**
   If the gate already passes, the task is done — mark it complete in the
   plan, commit, and move to the next task. Do NOT assume; verify.

4. **Read relevant spec files** if the task references one.

## Loop Unit
Each loop must work on one small, concrete unit of progress only.
Examples:
- add inventory manifest generation
- implement UTC normalization helper
- add hourly resampling test
- implement FFMC calculation wrapper

Non-examples:
- finish the pipeline
- build all FWI logic
- do all redundancy analysis

## Verification Gates (anti-pattern: self-referential trust)

Every task in IMPLEMENTATION_PLAN.md has a `gate` field — a command
that must exit 0 for the task to be considered done.

The loop may mark a task done ONLY if:
1. It ran the gate command
2. The gate exited 0

No diary entry, no checkbox, no "the file exists" observation counts
as proof. Run the gate.

### Gate tiers (in order of trust)
| Tier | Mechanism | Trust |
|---|---|---|
| Artifact exists | File path check | Low |
| Artifact valid | Schema/test/checksum | Medium |
| Behavior correct | Integration test | High |

## Completion Rule
Each loop must either:
- produce a passing commit (gate + ruff + pytest), or
- stop and report a validated blocker

## Self-Heal Budget
A blocker must not be reported on first friction.
Per loop, allow bounded self-healing before escalation:
- up to 3 repair attempts for the same failing step
- up to 2 strategy pivots when the first approach is wrong

## Transient Issues
Treat these as retryable unless they persist beyond the self-heal budget:
- exec hiccups
- path mistakes
- parser edge cases
- temporary rate limits
- lint or formatting failures
- dependency/import mistakes
- local test failures caused by the latest edit

## Real Blockers
Escalate only after retries when issues such as these remain:
- required data is missing or corrupted
- assignment ambiguity blocks correct implementation
- authentication or credentials are required
- repeated failures suggest a spec contradiction
- repository state appears unsafe or inconsistent

## Validation Backpressure
Before commit, the loop must run:
- `ruff check .` — must pass
- `pytest` — must pass
- the current task's gate — must pass

## Code Quality Rules
- style target line length: 50
- hard line length enforced by tooling: 80
- target McCabe complexity: less than 10
- hard McCabe limit: 15
- public functions require type hints
- important logic must not live only in notebook cells

## State File: ralph-state.json

Machine-maintained by `scripts/sync_state.py` and the pre-commit hook.
The loop reads it (via sync_state.py output) but should not hand-edit it.

Fields:
- `phase` / `phase_name` — derived from git history
- `last_commit` — current HEAD
- `tasks_completed` / `tasks_remaining` — from IMPLEMENTATION_PLAN.md
- `next_task` — first unchecked task with gate
- `status` — "running" or "paused"
- `blocker` — null or description
- `date` / `max_per_day` / `iteration` — loop cadence control

## Diary Requirement
At the end of each successful loop, append a short diary entry under
`docs/diary/` using Option C style:
- factual summary of work completed
- brief reflective note or uncertainty

The diary is append-only. The loop never reads it for state — it is
an audit log, not a decision source.

## Standup Rhythm
The system is high-autonomy, not fully unsupervised.
Standup-style check-ins should surface:
- what changed
- what passed or failed
- current milestone
- blockers or decisions needed

## Sprint Dependency Protocol

Before any sprint begins work, it MUST verify its prerequisites.

Each milestone/sprint has an explicit prerequisite list.
The sprint agent must:
1. read the prerequisite checklist below
2. verify each prerequisite exists in the repo (code, tests, artifacts)
3. if ANY prerequisite is missing:
   - do NOT begin work
   - report a dependency blocker
   - list which prerequisites are unmet
   - suggest which sprint must run first

### Prerequisite Checks

| Sprint / Milestone | Prerequisites | How to Verify |
|---|---|---|
| Imputation | `src/pea_met_network/resampling.py` exists and has tests | `test_resampling` passes |
| Cleaned outputs | Imputation module exists with tests | `test_imputation` passes |
| Stanhope cache | Repo scaffold, specs exist | file existence check |
| Benchmark alignment | Cleaned station outputs + Stanhope reference | processed CSVs exist |
| FWI input contract | Cleaned daily outputs for Cavendish + Greenwich | daily CSVs exist |
| FWI moisture codes | FWI input contract defined | spec + contract exist |
| FWI full chain | Moisture codes validated | FFMC/DMC/DC tests pass |
| Redundancy (PCA) | Cleaned outputs + Stanhope benchmark | benchmark artifacts exist |
| Redundancy (clustering) | Cleaned outputs + Stanhope benchmark | benchmark artifacts exist |
| Uncertainty analysis | Redundancy results exist | cluster/PCA outputs exist |

### Out-of-Order Protection

If a sprint fires before its prerequisites are satisfied:
- it MUST stop immediately
- it MUST NOT attempt to implement the missing prerequisites itself
- it MUST report which sprint should run first

## Repository Topology Guardrail

The repository root must remain clean and predictable.

Allowed root files:
- `README.md`
- `pyproject.toml`
- `requirements.txt`
- `.gitignore`
- `.pre-commit-config.yaml`
- `IMPLEMENTATION_PLAN.md`
- `cleaning.py` (only if required for assignment-facing execution)

Allowed root directories:
- `src/`
- `tests/`
- `docs/`
- `data/`
- `notebooks/`
- `specs/`
- `scripts/`

All other files or directories at repo root are violations unless the
working agreement and tests are updated first in the same change.
