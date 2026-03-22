# Redundancy Analysis Spec

## Goal
Assess whether any weather stations are redundant using more than one
analytical view, while remaining assignment compliant.

## Required Methods
Use both:
- PCA
- clustering

PCA alone is not sufficient for a redundancy recommendation.

## Benchmarking Requirement
Statistically compare park stations against the ECCC Stanhope reference
station to quantify similarity where overlap allows.

## Analytical Expectations
The analysis should include, where supported by the data:
- correlation or similarity structure
- variance overlap
- clustering behavior among stations
- time-series comparisons
- distance or similarity summaries relative to Stanhope

## Recommendation Principle
Do not recommend removing a station based on a single metric.
A removal or retention recommendation must synthesize:
- PCA evidence
- clustering evidence
- reference benchmarking
- uncertainty analysis
- any known data quality caveats

## Acceptance Criteria

These are machine-verifiable constraints. Ralph's implementation must satisfy
every item below. UnRalph will verify each one. A heuristic or simplification
does not satisfy a criterion that names a specific method.

### AC-RED-1: PCA Method
- Must use `sklearn.decomposition.PCA` (or equivalent with comparable output).
- Must operate on real station feature data (not synthetic/random data).
- Must produce interpretable output: component loadings, explained variance ratio.
- Test must verify that PCA output has the expected shape and variance structure.

### AC-RED-2: Clustering Method
- Must use a real clustering algorithm (e.g.
  `sklearn.cluster.AgglomerativeClustering`, `sklearn.cluster.KMeans`).
- Sorting, grouping by label, or any non-clustering heuristic does NOT satisfy this.
- Distance metric must be appropriate for correlation structure (e.g. correlation
  distance, not Euclidean on raw values).
- Test must verify that stations assigned to the same cluster are actually more
  similar than stations in different clusters.

### AC-RED-3: Stanhope Benchmark
- Must statistically compare park stations against the ECCC Stanhope reference.
- Must produce per-station similarity scores relative to Stanhope.
- Must use real station data (not synthetic).
- Test must verify that Stanhope comparison output exists and contains
  per-station metrics.

### AC-RED-4: Recommendation Synthesis
- Recommendations must combine evidence from PCA, clustering, AND benchmarking.
- A recommendation must NOT be based on a single metric alone.
- Output must include per-station recommendation (keep/remove/defer) with
  supporting evidence citations.
- Test must verify that recommendation output references at least two
  analytical methods.

### AC-RED-5: Test Quality
- Tests must use real project data or clearly documented synthetic data that
  mimics real station structure.
- Tests must verify behavior (correct clustering, correct PCA structure),
  not just file existence or return type.
- All tests must pass with `.venv/bin/pytest tests/test_redundancy.py -q`.

---

## Acceptance Criteria (Legacy)
This spec is satisfied when:
- PCA outputs exist and are interpretable
- clustering outputs exist and are interpretable
- benchmarking to Stanhope is documented
- station redundancy recommendations are evidence-based and qualified
