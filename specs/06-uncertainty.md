# Uncertainty Spec

## Goal
Quantify the uncertainty of station-removal recommendations and estimate
the probability of losing critical micro-climate information.

## Required Framing
Use a probabilistic method such as KDE or another defensible distributional
approach to express uncertainty.

## Target Output
Provide an interpretable estimate of the probability that removing a
station would lose important micro-climate information.

## Requirements
The uncertainty layer must:
- be tied to actual station similarity or divergence evidence
- surface assumptions clearly
- avoid false precision
- express limitations where sample size or coverage is weak

## Interpretation Rules
Outputs should be understandable by a technical report audience and
support stakeholder-facing recommendations.

## Acceptance Criteria

These are machine-verifiable constraints. Ralph's implementation must satisfy
every item below. UnRalph will verify each one. A heuristic or simplification
does not satisfy a criterion that names a specific method.

### AC-UNC-1: Probabilistic Method
- Must use a probabilistic/distributional method such as
  `scipy.stats.gaussian_kde` or equivalent.
- A weighted heuristic score, arithmetic formula, or arbitrary thresholds
  does NOT satisfy this criterion.
- The method must produce a distribution, not a single scalar score.

### AC-UNC-2: Station-Removal Risk
- Must quantify or bound the probability of losing critical micro-climate
  information if each station is removed.
- Output must be per-station (not a single global metric).
- Must be tied to actual station similarity or divergence evidence.

### AC-UNC-3: Confidence Intervals
- Must produce confidence intervals or credible intervals for risk estimates.
- Intervals must widen appropriately with fewer data points or shorter
  overlap periods.
- Test must verify interval behavior (narrow with lots of data, wide
  with little data).

### AC-UNC-4: Assumptions and Limitations
- Must surface assumptions clearly in output (documented in code or
  function docstrings).
- Must flag stations where data is insufficient for reliable uncertainty
  estimation.
- Must avoid false precision.

### AC-UNC-5: Integration with Recommendations
- Uncertainty estimates must be incorporated into final station
  recommendations from Phase 5.
- Recommendation output must reference uncertainty estimates.

### AC-UNC-6: Test Quality
- Tests must verify distributional behavior (e.g., KDE output shape,
  confidence interval properties), not just return types.
- Tests must use real project data or clearly documented synthetic data.
- All tests must pass with `.venv/bin/pytest tests/test_uncertainty.py -q`.

---

## Acceptance Criteria (Legacy)
This spec is satisfied when:
- a probabilistic uncertainty method is implemented
- station-removal risk is quantified or bounded
- the resulting uncertainty is incorporated into final recommendations
