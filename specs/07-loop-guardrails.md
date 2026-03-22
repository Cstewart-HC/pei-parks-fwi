# Autonomous Loop Guardrails

## Goal
Define safe, high-autonomy operating rules for the Ralph-style build loop
with adversarial validation (tick-tock).

## Architecture: Tick-Tock

Two agents, two roles, two models:
- **Ralph (TICK)** — builds, tests, commits. Model: gpt-5.3-codex.
- **UnRalph (TOCK)** — reviews, verifies, rejects weak work. Model: glm-5-turbo.

Different model families = different blind spots. Where one agent might
accept a shortcut, the other is more likely to catch it.

## Single Source of Truth: ralph-state.json

All phase state, decisions, and loop control live in
`docs/ralph-state.json`. This is the only file the loop reads for
state. It is machine-readable JSON.

The JSON contains:
- `phase` — current phase number (human-set, not derived)
- `phases` — phase definitions with names, exit criteria, status
- `decisions[]` — architectural decisions with dates and rationale
- `status` — "running" or "paused"
- `blocker` — null or description
- `date` / `max_per_day` / `iteration` — loop cadence control

## Validation: validation.json

`docs/validation.json` is the communication channel between Ralph and
UnRalph.

- **UnRalph writes it.** Ralph reads it (read-only).
- Contains: verdict (PASS/REJECT), per-criterion evidence, summary.
- Ralph must address all REJECT criteria before UnRalph will pass.

## Human-Readable Views

The JSON can generate markdown views on demand:
```
python scripts/sync_state.py --view plan       → task plan summary
python scripts/sync_state.py --view decisions  → decisions log
python scripts/sync_state.py --view status     → current state
```
These are for human eyes only. Never committed. Never read by the loop.

## Loop Startup Procedure (every Ralph iteration)

1. **Run `python scripts/sync_state.py`**
   Reads the JSON, discovers tests, reports working tree status,
   checks phase exit criteria.

2. **Read `docs/validation.json`**
   - VERDICT=REJECT → fix what UnRalph flagged
   - VERDICT=PASS or NONE → proceed with next gap

3. **Read `docs/loop-prompt.md`** for the full protocol.

4. **Read relevant spec files** for acceptance criteria context.

## Loop Startup Procedure (every UnRalph iteration)

1. **Read `docs/unralph-prompt.md`** for the full protocol.
2. **Read the spec** for the current phase.
3. **Read Ralph's code and tests.**
4. **Check each acceptance criterion** against the implementation.
5. **Write `docs/validation.json`** with verdict and evidence.

## Acceptance Criteria (human-defined)

Each spec has a machine-verifiable Acceptance Criteria section.
These are constraints that name specific methods, behaviors, and
test quality requirements. UnRalph verifies them. Ralph builds to them.

Key principle: a heuristic does not satisfy a criterion that names
a specific method. "Use sklearn clustering" means sklearn clustering,
not sorting. "Use KDE" means KDE, not weighted averages.

## Verification Gates (anti-pattern: self-referential trust)

Ralph must run verification before committing:
- `ruff check .` — must pass
- `pytest` (full suite) — must pass

UnRalph must run verification during review:
- `pytest tests/ -q` — must pass for PASS verdict
- Code inspection against acceptance criteria

### Auto-correction
sync_state.py automatically detects drift:
- Discovers test files and reports their status
- Checks phase exit criteria
- Reports working tree state

Run `python scripts/sync_state.py --check-only` to audit without writing.

## Completion Rule
Each Ralph loop must either:
- produce a passing commit (ruff + pytest), or
- stop and report a validated blocker

Each UnRalph loop must:
- produce a validation.json with clear verdict and evidence

## Self-Heal Budget
Per Ralph loop:
- up to 3 repair attempts for the same failing step
- up to 2 strategy pivots when the first approach is wrong

## Validation Backpressure
Before commit, the loop must run:
- `ruff check .` — must pass
- `pytest` — must pass

## Code Quality Rules
- style target line length: 50
- hard line length enforced by tooling: 80
- target McCabe complexity: less than 10
- hard McCabe limit: 15
- public functions require type hints

## Diary Requirement
Append-only audit log under `docs/diary/`. The loop never reads it
for state — it is an audit log, not a decision source.

## Standup Rhythm
Standup-style check-ins are handled externally (heartbeat),
not by Ralph or UnRalph. Neither agent delivers summaries.
