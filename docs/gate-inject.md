# Gate Results Block

_Generated: 2026-03-29T22:43:40.878201+00:00_

## Artifact Validation
- Verdict: SKIP
- Summary: Phase 11 has no data artifact expectations
- Errors: none

## Pre-Flight
- Verdict: SKIP
- Summary: Phase 11 has no structural requirements
- Missing: none

## Martin Lint
- Verdict: FAIL
- Summary: 83 violation(s) across 23 file(s): 1 high, 47 medium, 35 low
- Violations: 83 total
  - high AP002 tests/test_phase11_dual_mode_fwi.py:187 — [unmarked_subprocess] result = subprocess.run(
  - medium AP027 tests/test_analysis_notebook.py:72 — [complex_test_logic] 'test_notebook_executes_without_errors' has 4 branches (max 3)
  - medium AP006 tests/test_analysis_notebook.py:85 — [shared_state_write] """AC-ANA-1: Notebook references data/processed/ files."""
  - medium AP007 tests/test_cross_station_impute.py:151 — [duplicate_test_name] 'test_capped_at_100' also defined at line 110
  - medium AP014 tests/test_cross_station_impute.py:190 — [float_assertion_without_approx] assert st.anemometer_height_m == 10.0
  - medium AP005 tests/test_cross_station_impute.py:252 — [tautological_assertion] assert ts.tz is not None
  - medium AP005 tests/test_cross_station_impute.py:373 — [tautological_assertion] assert hc.empirically_derived is True
  - medium AP005 tests/test_cross_station_impute.py:752 — [tautological_assertion] assert result is not None
  - medium AP005 tests/test_cross_station_impute.py:753 — [tautological_assertion] assert result.empirically_derived is True
  - medium AP005 tests/test_cross_station_impute.py:773 — [tautological_assertion] assert result is None
  - ... 73 more violation(s) in docs/martin-lint.json
