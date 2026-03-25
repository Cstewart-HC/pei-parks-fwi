# The MissHoover Pattern

> A battle-tested autonomous software development system with adversarial review.
> Evolved from Geoffrey Huntley's Ralph Loop through 63 loops and 102 commits.

---

## 1. Executive Summary

The MissHoover Pattern is an autonomous development loop where a single agent operates in three modes — **MissHoover** (orchestrator), **Ralph** (builder), and **Lisa** (reviewer) — to iteratively build software against human-reviewed specifications.

**Key properties:**
- Single agent, three roles, one cron job
- Adversarial review prevents self-approval
- Deterministic state management via scripts and hooks, not prompt instructions
- Phase-gated progress with machine-checked exit criteria
- Circuit breaker prevents runaway loops

**One-line summary:** Program the loop, not the software. Your job is designing the control system that builds the software — specs, prompts, state transitions, guardrails, and failure recovery.

---

## 2. Genesis and Evolution

### Origin
This pattern started as an implementation of Geoffrey Huntley's [Ralph Loop](https://ghuntley.com/loop/) — a simple concept: put a coding agent in a `while true` loop with specs and let it build autonomously. Huntley's key insight: "everything is a ralph loop" — the context window is an array, and deterministic allocation of that array avoids context rot and compaction.

### What we adopted from the original
- Monolithic single-repo, single-process approach (not multi-agent)
- "One task per loop" discipline (relaxed for fix batching)
- Specs as the contract — human decisions, machine verification
- Fresh context per role — don't mix concerns in one session
- "Clay on the pottery wheel" — iterative refinement of specs

### What we invented
- **Adversarial review (Lisa)** — a separate review mode that assumes code is wrong until proven
- **Phase-gated state machine** — explicit phases with exit commands and a 2×2 advancement grid
- **Single orchestrator with dynamic prompt injection** — one cron job decides who runs
- **AgentEnd hook for deterministic pointer management** — state updates happen via infrastructure, not prompt instructions
- **Circuit breaker** — 3-stall detection with automatic trip
- **Docs-only commit filtering** — infra commits skip review

### Evolution timeline
| Stage | What happened |
|---|---|
| Loops 1–10 | Manual Ralph runs, no Lisa, no state machine |
| Loops 11–25 | Added Lisa (adversarial review), separate cron jobs |
| Loops 26–40 | Added orchestrator (single job), phase gates, sync_state.py |
| Loops 41–50 | Fixed iteration budgets, preset issues, infinite loop bugs |
| Loops 51–63 | AgentEnd hook, deliverables phase, project completion |

---

## 3. Architecture Overview

```
                    ┌─────────────────────────────────┐
                    │        CRON (every 15 min)       │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │     MISSHOOVER (orchestrator)    │
                    │                                  │
                    │  1. Read state + git log         │
                    │  2. Apply decision tree          │
                    │  3. Become Ralph OR Lisa         │
                    │  4. Do the work                  │
                    │  5. Exit                         │
                    └──────┬────────────────┬──────────┘
                           │                │
                    ┌──────▼──────┐  ┌──────▼──────┐
                    │   RALPH     │  │    LISA     │
                    │  (builder)  │  │  (reviewer) │
                    │             │  │             │
                    │ • Read spec │  │ • Read spec │
                    │ • Write test│  │ • Inspect   │
                    │ • Implement │  │   commits   │
                    │ • Verify    │  │ • Run tests │
                    │ • Commit    │  │ • Evaluate  │
                    │             │  │   criteria  │
                    │             │  │ • Write     │
                    │             │  │   verdict   │
                    └──────┬──────┘  └──────┬──────┘
                           │                │
                    ┌──────▼────────────────▼──────┐
                    │       AGENTEND HOOK           │
                    │  (deterministic, not prompt)  │
                    │                               │
                    │  • Capture HEAD hash          │
                    │  • Update last_reviewed_commit│
                    │  • Commit if changed          │
                    └──────────────┬────────────────┘
                                   │
                    ┌──────────────▼────────────────┐
                    │       STATE FILES              │
                    │                               │
                    │  ralp-state.json              │
                    │  validation.json              │
                    └───────────────────────────────┘
```

### Data flow
```
Specs (human) → Ralph (build) → Commits → Lisa (review) → validation.json
                                                         │
                    sync_state.py ←──────────────────────┘
                         │
                    ralp-state.json (phase transitions)
                         │
                    Orchestrator reads both on next tick
```

---

## 4. The Three Roles

### MissHoover (Orchestrator)
The orchestrator is not a separate agent — it's a **mode** that the single agent enters. It reads state, makes a decision, loads the appropriate prompt, and becomes Ralph or Lisa for the duration of that tick.

**Rules:**
- Read state files and git log first
- Apply the 5-rule decision tree (see Section 6)
- Load the appropriate prompt file
- Do NOT manage `last_reviewed_commit` — the hook handles that
- Do NOT modify `ralph-state.json` directly — `sync_state.py` handles that
- Deliver results to the appropriate Discord channel

### Ralph (Builder)
Ralph reads specs, writes tests, implements code, verifies, and commits.

**Greenfield mode** (no active REJECT):
- One task per run
- One test group
- One implementation slice
- One commit

**Reject-repair mode** (Lisa has rejected specific criteria):
- Multiple fixes allowed in one run
- Each fix must be independently verified
- Commit immediately after each fix passes verification
- This prevents lost work if the iteration budget runs out

**Rules:**
- Run `sync_state.py` first
- Consult `specs-readme.md` before searching for context
- Implementation must match specs, not vibes
- Tests must prove behavior, not mere existence
- Run lint and tests before every commit
- Inspect git diff before committing
- Never modify `ralph-state.json` or `validation.json` directly
- If blocked after repeated attempts, record a blocker and stop

### Lisa (Reviewer)
Lisa exists to be adversarial, not polite. Her job is to prove the implementation wrong unless the evidence is strong.

**Checklist:**
1. Read current phase and active specs
2. Inspect recent commits
3. Read changed code and surrounding modules
4. Inspect tests critically — would they catch a fake implementation?
5. Run the relevant test suite
6. Evaluate each acceptance criterion individually
7. Write `validation.json` with verdict, per-criterion findings, and summary

**What Lisa looks for:**
- Does the implementation actually use the required method (not just import it)?
- Does the test verify behavior (not just existence or structure)?
- Would the test fail if the implementation were wrong?
- Is evidence direct or hand-wavy?
- Did the fix solve the root issue or just the visible symptom?

**Verdict meanings:**
- **PASS**: All criteria satisfied, tests pass meaningfully, no spec violations
- **REJECT**: At least one criterion fails — include specific, actionable findings

---

## 5. The State Machine

### `ralph-state.json`
```json
{
  "iteration": 0,
  "date": "YYYY-MM-DD",
  "max_per_day": 20,
  "phase": 1,
  "status": "running",
  "blocker": null,
  "updated_at": null,
  "phases": {
    "1": {
      "name": "Phase Name",
      "exit": "pytest tests/test_phase1.py -q",
      "status": "active"
    }
  },
  "decisions": [],
  "circuit_breaker": {
    "consecutive_stalls": 0,
    "last_commit": null,
    "last_verdict": null,
    "last_fail_criteria_hash": null,
    "consecutive_errors": 0,
    "tripped": false,
    "trip_reason": null,
    "trip_at": null
  }
}
```

### `validation.json`
```json
{
  "last_reviewed_commit": "abc1234",
  "verdict": "NONE",
  "reviewed_at": null,
  "criteria": [],
  "summary": "No review yet"
}
```

### Phase advancement: the 2×2 grid

| Phase Exit | Validation | Meaning | Action |
|---|---|---|---|
| PASS | PASS | True pass | Advance to next phase |
| PASS | REJECT | False green | Block — tests pass but review caught issues |
| FAIL | REJECT | Normal work-in-progress | Continue — Ralph needs to keep working |
| FAIL | PASS | Anomaly | Log and hold — unexpected state |

### Circuit breaker

Trip conditions:
- 3 consecutive stalls (same commit, same rejection, same failing criteria)
- Repeated loops without meaningful movement

When tripped:
- All roles stop
- Orchestrator escalates to DMs
- Human investigation required

Reset conditions:
- Manual human reset
- New commits land (evidence of progress)

---

## 6. Decision Tree

The orchestrator follows exactly 5 rules, evaluated in order:

### Rule 1: Circuit breaker
```
IF circuit_breaker.tripped == true:
    send escalation to DMs
    STOP
```

### Rule 2: New code commits since last review
```
commits = git log LAST_REVIEWED..HEAD -- src/ tests/ notebooks/ README.md cleaning.py
# Exclude: docs/, specs/, scripts/, config files

IF commits is not empty:
    Become Lisa
    Run adversarial review
    Write validation.json with verdict
    STOP
```

**Why exclude `docs/`, `specs/`, `scripts/`, and config files?**
Infra commits (prompts, state syncs, spec updates) don't change code behavior. Running Lisa on them wastes iterations and creates infinite review loops. Only code changes need review.

### Rule 3: REJECT with no new commits
```
IF verdict == "REJECT" AND no new code commits:
    Become Ralph
    Fix rejected criteria
    Commit each fix immediately after verification
    STOP
```

### Rule 4: PASS with no new commits
```
IF verdict == "PASS" AND no new code commits:
    Run sync_state.py
    IF state changed:
        Commit updated state
    STOP
```

### Rule 5: No review yet
```
IF verdict == "NONE" OR no validation exists:
    Become Ralph
    Start working on active phase
    STOP
```

---

## 7. Infrastructure Components

### `sync_state.py`
The state machine operator. Runs at the start of every Ralph loop and during orchestrator sync.

**Responsibilities:**
- Read `ralph-state.json` and `validation.json`
- Detect dirty state files and restore from HEAD if needed
- Evaluate current phase exit command
- Combine phase-exit result with validation verdict using the 2×2 grid
- Advance, block, continue, or flag anomaly
- Update circuit-breaker counters
- Auto-clear stale blockers when appropriate

**Ownership:** This script owns `ralph-state.json`. Humans should not manually edit phase state except for infrastructure recovery.

### AgentEnd Hook (`scripts/orchestrator_end_hook.py`)

This is the most critical infrastructure decision we made.

**Problem it solves:** The orchestrator agent kept failing to update `last_reviewed_commit` in `validation.json`. Whether due to prompt complexity, iteration budget exhaustion, or model confusion, the agent would commit its work but leave the pointer stale. This caused infinite loops — the next tick would see "new commits since last review" and re-review or re-sync indefinitely.

**Solution:** Move pointer management out of the agent entirely. An `AgentEnd` hook fires after every orchestrator run and deterministically:
1. Captures current git HEAD
2. Updates `last_reviewed_commit` in `validation.json`
3. Commits if changed

**Why a hook, not a prompt instruction?**
- Prompts are non-deterministic — the agent may skip, misinterpret, or partially execute instructions
- Hooks are deterministic — they run regardless of what the agent did
- Hooks fire after the agent exits, so they can clean up any state the agent left behind
- This follows the principle: "if it can go wrong, make it impossible to go wrong"

### Pre-commit Hook
Runs `sync_state.py` on every commit. Ensures state is continuously normalized even during manual commits.

### Cron Configuration
Single job, 15-minute cadence:
```json
{
  "name": "orchestrator-pea-met-network",
  "schedule": "0,15,30,45 * * * *",
  "payload": {
    "kind": "agentTurn",
    "model": "custom-z-ai::glm-5-turbo",
    "message": "Working directory: /path/to/project\n\nRead docs/orchestrator-prompt.md and follow it exactly.",
    "timeout_secs": 900,
    "deliver": true,
    "channel": "<bot_account_id>",
    "to": "<channel_chat_id>"
  },
  "execution": { "target": "host" },
  "sessionTarget": "isolated"
}
```

### Preset Configuration
```toml
[agents.presets.miss-hoover]
delegate_only = false
max_iterations = 35
timeout_secs = 900

[agents.presets.miss-hoover.tools]
allow = ["exec", "mcp__filesystem__*", "calc", "send_message", "session_state"]
deny = ["spawn_agent", "browser", "memory_save", "cron"]
```

**Critical:** The orchestrator must NOT use a `delegate_only = true` preset. The default `orchestrator` preset in Moltis has `delegate_only = true` and `max_iterations = 20`, which blocks `exec` and caps iterations too low. Always use a dedicated preset.

### Prompt Files
Three prompt files under `docs/`:
- `docs/orchestrator-prompt.md` — decision logic
- `docs/ralph-prompt.md` — Ralph's instructions
- `docs/lisa-prompt.md` — Lisa's instructions

**Rule for prompts:** Every section should justify itself. For each section, ask: what failure does this prevent? What context does this provide? What happens if it is removed?

### `specs-readme.md` (The Pin)
A lookup table that maps concepts to synonyms, spec files, test files, and source files. This improves search tool hit rate and reduces hallucination.

Example:
```markdown
## Authentication
Aliases: auth, authn, login, sign-in, session, identity
Spec: specs/03-auth.md
Tests: tests/test_auth.py
Source: src/app/auth.py
```

### State Integrity Rules
1. `ralph-state.json` is owned by `sync_state.py`. Do not manually edit for normal progress.
2. `validation.json` is written by Lisa and the AgentEnd hook. Do not manually edit.
3. The AgentEnd hook must always run after the orchestrator. If it fails, the loop will break.
4. State files are committed to the repo. `sync_state.py` can restore them from HEAD if dirty.

---

## 8. Lessons Learned (Bugs We Actually Hit)

### Bug 1: Orchestrator preset blocks exec
**Symptom:** Agent can read/write files but cannot run shell commands. No git, no tests, no commits.
**Root cause:** Default preset was `orchestrator` with `delegate_only = true` and `max_iterations = 20`.
**Fix:** Created dedicated `miss-hoover` preset with `delegate_only = false` and `max_iterations = 35`.
**Lesson:** Always verify the preset. The default routing preset is designed for delegation, not execution.

### Bug 2: Infinite Lisa loop
**Symptom:** Lisa reviews the same commit repeatedly, burning iterations every tick.
**Root cause:** After committing Lisa's verdict, `last_reviewed_commit` still pointed to the reviewed commit (not the verdict commit). The verdict commit was seen as "new work" → trigger Lisa again.
**Fix:** Capture HEAD before committing, set `last_reviewed_commit` to the pre-commit hash.
**Lesson:** The pointer must track what was reviewed, not the verdict itself. The verdict commit is an artifact, not code.

### Bug 3: Infinite sync loop
**Symptom:** Orchestrator commits sync state, then re-syncs on next tick, forever.
**Root cause:** Same as Bug 2 but for sync commits. `last_reviewed_commit` wasn't updated after sync.
**Fix:** AgentEnd hook now updates the pointer after every orchestrator run.
**Lesson:** Any commit the orchestrator makes can trigger re-evaluation. The pointer must advance after every run, regardless of what the orchestrator did.

### Bug 4: AgentEnd hook needed instead of prompt instructions
**Symptom:** Despite multiple prompt rewrites, the agent kept failing to update `last_reviewed_commit`.
**Root cause:** Prompt instructions are non-deterministic. The agent would sometimes skip the teardown step, especially when iteration budget was tight.
**Fix:** Moved pointer management to an `AgentEnd` hook that runs deterministically after every orchestrator run.
**Lesson:** If state management is critical, don't trust the agent to do it. Use infrastructure.

### Bug 5: Docs-only commits trigger Lisa
**Symptom:** Infra commits (prompt changes, spec updates) cause Lisa to review, burning iterations on non-code changes.
**Root cause:** Decision tree checked all commits, not just code commits.
**Fix:** Filter commits — only `src/`, `tests/`, `notebooks/`, `README.md`, and root scripts trigger Lisa. Exclude `docs/`, `specs/`, `scripts/`, and config files.
**Lesson:** Not all commits are equal. Infra commits should advance the pointer without review.

### Bug 6: Phase advance blocked by missing test file
**Symptom:** `sync_state.py` won't advance to Phase 8 because `test_deliverables.py` doesn't exist yet. But Ralph can't create it until Phase 8 is active.
**Root cause:** `sync_state.py` checks the exit command of the TARGET phase, not just the current phase.
**Fix:** Only check exit command for the current phase. Advancing to a new phase should always succeed if the current phase passes.
**Lesson:** State transitions should only validate what's leaving, not what's entering.

### Bug 7: `git commit --amend` invalidates tracked hashes
**Symptom:** Amending a commit to include `last_reviewed_commit` changes the commit hash, instantly invalidating the value we just saved.
**Root cause:** Modifying a tracked file after committing changes the tree hash, which changes the commit hash.
**Fix:** Single-commit pattern — capture HEAD before changes, update files, commit once.
**Lesson:** Never amend commits that track their own hashes. Use a capture-then-commit pattern.

### Bug 8: Duplicate cron jobs from failed cleanup
**Symptom:** Two orchestrator jobs running simultaneously, competing and producing conflicting state.
**Root cause:** Cron job deletion via API failed silently. Job was recreated without realizing the original still existed.
**Fix:** Direct database cleanup (delete from `cron_runs` then `cron_jobs` tables), plus Moltis restart to clear in-memory state.
**Lesson:** Always verify job cleanup with `list` after deletion. If the API fails, check the database directly.

### Bug 9: UnRalph job wouldn't die
**Symptom:** Disabled cron job kept producing runs despite `enabled: false`.
**Root cause:** Job existed in the API's in-memory state but not in `jobs.json` or the database. API `list` returned stale data.
**Fix:** Moltis restart cleared the in-memory state. Job was a phantom.
**Lesson:** After container restarts, verify the job list matches what's on disk. Stale in-memory state can survive restarts if not properly reloaded.

---

## 9. Bootstrap Checklist

### Project Setup
- [ ] 1. Define the project goal and scope
- [ ] 2. Hold a spec conversation with the LLM
- [ ] 3. Generate initial specs and implementation plan
- [ ] 4. Review and harden specs by hand
- [ ] 5. Create `specs-readme.md` with synonyms and file mappings
- [ ] 6. Set deterministic repo structure
- [ ] 7. Create `docs/ralph-state.json` with phase definitions
- [ ] 8. Create `docs/validation.json` with empty initial state
- [ ] 9. Write `docs/ralph-prompt.md` (Ralph's instructions)
- [ ] 10. Write `docs/lisa-prompt.md` (Lisa's instructions)
- [ ] 11. Write `docs/orchestrator-prompt.md` (decision tree)

### Infrastructure
- [ ] 12. Create `scripts/sync_state.py` (state machine operator)
- [ ] 13. Create `scripts/orchestrator_end_hook.py` (AgentEnd hook)
- [ ] 14. Register the AgentEnd hook in Moltis configuration
- [ ] 15. Install pre-commit hook
- [ ] 16. Create `miss-hoover` preset with correct tool permissions
- [ ] 17. Set `default_preset = "miss-hoover"` in Moltis config (or assign preset to cron job)

### Validation
- [ ] 18. Run a few manual/attended loops
- [ ] 19. Verify Lisa produces meaningful reviews
- [ ] 20. Verify Ralph commits green work
- [ ] 21. Verify the AgentEnd hook advances the pointer
- [ ] 22. Verify sync_state.py advances phases correctly
- [ ] 23. Verify the circuit breaker trips on stalls

### Launch
- [ ] 24. Create single orchestrator cron job
- [ ] 25. Configure Discord delivery to project channel
- [ ] 26. Configure escalation to DMs for circuit breaker trips
- [ ] 27. Watch early loops closely
- [ ] 28. Fix loop-level failure domains as they appear
- [ ] 29. Let it run unattended

### Project Completion
- [ ] 30. Add deliverables phase (README, entrypoint scripts, notebooks)
- [ ] 31. Add convention sweep phase if needed
- [ ] 32. Verify all phases pass
- [ ] 33. Disable cron job
- [ ] 34. Save lessons learned
- [ ] 35. Archive prompts and state files

---

## 10. Design Principles

1. **Specs first, code second** — Human decisions become machine-verified contracts
2. **Fresh context per role** — Don't mix orchestrator, builder, and reviewer in one session
3. **Monolith before multi-agent** — Single repo, single process, single cron job
4. **Backpressure beats clever prompting** — Tests, linters, adversarial review, circuit breakers
5. **Search linkage reduces hallucination** — `specs-readme.md` with synonyms improves retrieval
6. **Adversarial review is mandatory** — Ralph never self-approves
7. **State transitions must be mechanical** — `sync_state.py` owns the state machine
8. **Infrastructure over prompt instructions** — Hooks and scripts for critical state management
9. **Humans own specs and infra** — Not routine iteration
10. **Commit each fix immediately** — Don't batch fixes in reject-repair mode
11. **Single-commit pattern for pointer tracking** — Capture HEAD, update files, commit once
12. **Exclude infra commits from review** — Only code changes trigger Lisa
13. **Conventions can be a separate phase** — Don't block feature work on style
14. **Every loop failure is a design lesson** — Fix the loop, not just the code

---

## 11. Comparison with Original Ralph

| Aspect | Huntley's Ralph | MissHoover Pattern |
|---|---|---|
| Roles | Single role (Ralph) | Three modes (Hoover/Ralph/Lisa) |
| Review | None (self-approval) | Mandatory adversarial review (Lisa) |
| State tracking | None | Phase-gated state machine |
| Progress verification | Manual observation | Automated exit gates + 2×2 grid |
| Scheduling | `while true` in terminal | Single cron job with decision tree |
| Runaway prevention | Manual CTRL+C | Circuit breaker (3-stall trip) |
| Context management | Deterministic array allocation | Fresh session per tick + AgentEnd hook |
| Spec generation | Conversation → review | Same, plus specs-readme.md lookup table |
| Commit frequency | One per loop | One per fix (reject-repair mode) |
| Post-loop cleanup | Separate Ralph loop for conventions | Optional convention sweep phase |
| Failure recovery | "Another Ralph loop" | Circuit breaker + escalation to DMs |
| Infrastructure | Bash loop + Claude Code | Cron + sync_state.py + AgentEnd hook |

---

## 12. Caveats and Open Problems

### Known Limitations
1. **Model dependency** — The pattern works well with `glm-5-turbo` but may need prompt adjustments for other models. The orchestrator's role-switching (read prompt → become role) is cognitively demanding.
2. **Iteration budget** — 35 iterations works for our project. Larger projects may need more. Bumping too high risks runaway loops.
3. **No CI/CD** — The loop commits directly to the repo. There's no automated deployment pipeline. Huntley's Loom has autonomous deployment; we don't.
4. **Single-repo assumption** — The pattern assumes everything lives in one repo. Multi-repo projects would need adaptation.
5. **Discord dependency** — Delivery and escalation are tied to Discord. Other channels would need prompt and cron changes.

### Open Questions
1. **What's the right iteration budget?** We settled on 35 empirically. Is there a formula based on project complexity?
2. **Should the orchestrator be a separate agent?** We use one agent with dynamic prompt injection. A separate orchestrator agent (via `spawn_agent`) would give independent iteration budgets but adds complexity.
3. **How to handle merge conflicts?** If Lisa's review commit conflicts with Ralph's fix commit, the loop stalls. We haven't seen this yet but it's possible.
4. **How to handle spec changes mid-loop?** Specs are supposed to be frozen during the autonomous phase. But what if a critical spec error is discovered? There's no formal process for mid-loop spec updates.

### What We'd Do Differently
1. **Start with the hook** — We lost many loops to pointer management bugs before implementing the AgentEnd hook. Start with deterministic infrastructure from day one.
2. **Test the preset first** — Verify the preset gives the agent the right tools before writing any prompts. We wasted time debugging tool access issues.
3. **Smaller phases** — Our later phases (remediation, deliverables) were too broad. Smaller phases with tighter exit criteria would give faster feedback loops.
4. **Commit the lookup table earlier** — `specs-readme.md` was added late. Having it from the start would have reduced hallucination in early loops.

---

_This document is a living artifact. As the pattern is applied to new projects, it should be updated with new lessons, refinements, and caveats._
