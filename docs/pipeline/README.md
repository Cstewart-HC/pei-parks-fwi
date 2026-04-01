# PEA Met Network — Pipeline Documentation

Data processing pipeline for the DATA-3210 PEI meteorological network (6 weather stations).

## Documents

| File | Concern |
|---|---|
| [architecture.md](architecture.md) | Pipeline overview, two-pass design, execution flow |
| [ingestion.md](ingestion.md) | Raw data sources, adapter registry, column normalization |
| [quality-enforcement.md](quality-enforcement.md) | Value ranges, rate-of-change, cross-variable, flatline, enforcement actions |
| [imputation.md](imputation.md) | Intra-station gap strategy + cross-station donor imputation |
| [fwi.md](fwi.md) | Fire Weather Index calculation, modes, chain break diagnostics |
| [outputs.md](outputs.md) | File structure, quality flags, reports, known sensor gaps |

## Quick Reference

```
python -m pea_met_network --stations all
python -m pea_met_network --stations greenwich,cavendish
```

Config: `docs/cleaning-config.json`
Source: `src/pea_met_network/cleaning.py`
