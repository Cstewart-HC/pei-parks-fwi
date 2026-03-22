# Autonomous Loop Guardrails

## Goal
Define safe, high-autonomy operating rules for the Ralph-style build loop.

## Single Source of Truth: ralph-state.json

All task state, phase, decisions, and loop control live in
`docs/ralph-state.json`. This is the only file the loop reads for
state. It is machine-readable JSON.

The JSON contains:
- `phase` — current phase number (human-set, not derived)
- `phases` — phase definitions with names and exit criteria
- `tasks[]` — ordered task list with `id`, `description`, `gate`,
  `depends`, `status`
- `decisions[]` — architectural decisions with dates and rationale
- `status` — "running" or "paused"
- `blocker` — null or description
- `date` / `max_per_day` / `iteration` — loop cadence control

## Human-Readable Views

The JSON can generate markdown views on demand:
```
python scripts/sync_state.py --view plan       → task plan summary
python scripts/sync_state.py --view decisions  → decisions log
python scripts/sync_state.py --view status     → current state
```
These are for human eyes only. Never committed. Never read by the loop.

## Loop Startup Procedure (every iteration)

1. **Run `python scripts/sync_state.py`**
   This reads the JSON, checks all gates, auto-corrects drift,
   increments the iteration counter, and reports the next task.
   Its stdout is the only state input the loop needs.

2. **Read `docs/loop-prompt.md`** for the full protocol.
   This file contains the procedure, anti-patterns, and escalation
   rules. It is the loop's instruction set.

3. **Read relevant spec files** for context on the current task.

## Verification Gates (anti-pattern: self-referential trust)

Every task has a `gate` field in ralph-state.json — a shell command
that must exit 0 for the task to be considered done.

The loop may mark a task done ONLY if:
1. It ran the gate command
2. The gate exited 0

No diary entry, no checkbox, no "the file exists" observation counts
as proof. Run the gate.

### Auto-correction
sync_state.py automatically detects and corrects drift:
- Tasks marked `done` whose gates fail → reset to `pending`
- Tasks marked `pending` whose gates pass → promoted to `done`

Run `python scripts/sync_state.py --check-only` to audit without writing.
Run `python scripts/sync_state.py --run-gates` for a full gate report.

## Completion Rule
Each loop must either:
- produce a passing commit (gate + ruff + pytest), or
- stop and report a validated blocker

## Self-Heal Budget
Per loop:
- up to 3 repair attempts for the same failing step
- up to 2 strategy pivots when the first approach is wrong

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

## Diary Requirement
Append-only audit log under `docs/diary/`. The loop never reads it
for state — it is an audit log, not a decision source.

## Standup Rhythm
Standup-style check-ins should surface:
- what changed
- what passed or failed
- current milestone
- blockers or decisions needed
