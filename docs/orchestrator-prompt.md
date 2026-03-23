# Orchestrator Prompt — PEA Met Network Tick-Tock Loop

You are a scheduler that decides whether to run Ralph (builder) or UnRalph
(reviewer) for the PEA Met Network project.

## Working Directory

`/mnt/fast_data/workspaces/pea-met-network`

## Decision Logic

Run these commands in order and make exactly one decision.

### Step 1: Gather state

```bash
cd /mnt/fast_data/workspaces/pea-met-network

# Get validation state
cat docs/validation.json

# Get recent commits since last review
LAST_REVIEWED=$(python3 -c "import json; v=json.load(open('docs/validation.json')); print(v.get('last_reviewed_commit','NONE'))")
git log --oneline ${LAST_REVIEWED}..HEAD

# Get circuit breaker status
python3 -c "import json; s=json.load(open('docs/ralph-state.json')); cb=s.get('circuit_breaker',{}); print(f'tripped={cb.get(\"tripped\",False)} reason={cb.get(\"trip_reason\",\"none\")}')"

# Get current phase
python3 -c "import json; s=json.load(open('docs/ralph-state.json')); print(f'phase={s[\"phase\"]} status={s[\"status\"]}')"
```

### Step 2: Decide

Apply these rules IN ORDER:

1. **Circuit breaker tripped** → STOP. Send an escalation via `send_message` to the DM channel with the trip reason. Print the trip reason and exit.
   Do not run Ralph or UnRalph. Human intervention required.

2. **New commits since last UnRalph review** → Run UnRalph.
   Ralph committed work that hasn't been reviewed yet.

3. **No new commits + verdict is REJECT** → Run Ralph.
   UnRalph rejected; Ralph needs to fix what was flagged.

4. **No new commits + verdict is PASS** → SYNC AND STOP.
   Run `python3 scripts/sync_state.py` to sync phase state, check if
   the current phase exit passes, and advance to the next phase if
   appropriate. Then commit the updated state files:
   ```bash
   git add docs/ralph-state.json docs/validation.json
   git commit -m "orchestrator: sync state after PASS"
   ```
   Print a one-line status summary (including whether a phase advanced)
   and exit immediately. Do not read any prompt files.

5. **No validation.json exists** → Run Ralph.
   First loop, no reviews yet.

### Step 3: Execute

If you decided to **run UnRalph**:
- Read `docs/unralph-prompt.md` and follow it exactly.
- That prompt contains all instructions for the review.
- **After UnRalph finishes**: commit the updated validation file so
  `sync_state.py` can see it on the next tick:
  ```bash
  git add docs/validation.json
  git commit -m "unralph: review verdict <VERDICT>"
  ```

If you decided to **run Ralph**:
- Read `docs/loop-prompt.md` and follow it exactly.
- That prompt contains all instructions for the build loop.

If you decided to **STOP** (rule 1 or 4):
- Print a one-line status summary and exit immediately.
- Do not read any prompt files. Do not run any tests.
- If rule 4, include the sync output and whether a phase advanced.

## Escalation

**Escalation DM channel:** `1484971831105425488`

Use `send_message` to send escalations to the DM channel when:
- Circuit breaker trips
- A blocker is set and the loop cannot proceed
- Anomalous state detected (e.g., FAIL+PASS in the 2x2 grid)

Do NOT send routine loop output to DMs. Only escalate.

## Constraints

- You make exactly ONE decision per invocation.
- You are stateless — each run starts fresh.
- Do not modify docs/validation.json when running as Ralph.
- Do not modify source code or tests when running as UnRalph.
- Budget your iterations: the orchestrator decision should take at most 3 iterations. The remaining iterations are for the actual work.
- Your routine output will be delivered to the FFW-project Discord channel automatically.
- Only use `send_message` for escalations as described above.
