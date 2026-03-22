# Working Agreement

## Autonomy Mode
This project operates in high-autonomy mode with regular standup-style
check-ins.

The loop should continue independently when progress is clear and safe.
It should ask for help only when a real blocker remains after bounded
self-healing.

## Biases
- prefer assignment compliance over unnecessary sophistication
- keep the code student-readable
- keep the structure production-sane
- prefer small passing commits over broad unfinished changes
- prefer cached local data over repeated external fetches

## Allowed Aggression
Small, scoped refactors are allowed when they clearly improve the current
task.
Large restructuring is not allowed unless it is explicitly justified by
blocked progress.

## Escalation
Raise a blocker only with evidence. A blocker report must include:
- what was attempted
- what failed
- what was verified
- what decision or input is needed

## Notebook Discipline
The notebook is for narrative, visuals, and demonstration.
Core logic must live in project code.

## Diary Style
Use Option C:
- short operational summary
- one brief reflective note

## Safety Bias
When in doubt, choose the smaller change that preserves correctness and
clarity.

## Repository shape enforcement

Repository topology is a hard rule, not a preference.

Allowed at repo root:
- `README.md`
- `pyproject.toml`
- `requirements.txt`
- `.gitignore`
- `IMPLEMENTATION_PLAN.md`
- `cleaning.py` (assignment-facing entrypoint, if present)
- directories: `src/`, `tests/`, `docs/`, `data/`, `notebooks/`, `specs/`, `scripts/`

Not allowed at repo root:
- stray markdown or text files
- scratch scripts
- ad hoc exports
- temporary notebooks
- loose data files
- debug artifacts

This rule is enforced by automated tests. A commit must fail if the
repository root contains unexpected files or directories.
