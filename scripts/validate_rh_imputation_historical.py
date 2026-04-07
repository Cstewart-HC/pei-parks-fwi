#!/usr/bin/env python3
"""Historical RH imputation validation against ~2 years of processed data.

Greenwich has an RH sensor. We pretend it's missing and compare estimation
methods against the hidden truth:

  1. VP continuity from Cavendish (P1 — current method for RH-less stations)
  2. Direct RH from Cavendish (copy Cavendish RH, no VP transfer)
  3. Direct RH from North Rustico (P2 donor, copy)
  4. Mean of Cavendish + North Rustico direct RH
  5. Nearest-neighbor temperature ratio (RH × T_donor / T_target baseline)

Metrics: MAE, RMSE, bias, R² for each method, overall and by season.

Usage:
    cd /mnt/fast_data/workspaces/pea-met-network
    python scripts/validate_rh_imputation_historical.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("data/processed")


def load_station(name: str) -> pd.DataFrame:
    """Load hourly CSV for a station, set timestamp_utc as index."""
    p = DATA_DIR / name / "station_hourly.csv"
    df = pd.read_csv(p, parse_dates=["timestamp_utc"])
    df = df.set_index("timestamp_utc")
    df = df.sort_index()
    return df


def score(predicted: pd.Series, truth: pd.Series, label: str) -> dict:
    """Compute MAE, RMSE, bias, R² between predicted and truth."""
    valid = truth.notna() & predicted.notna()
    n = int(valid.sum())
    if n == 0:
        return {"label": label, "n": 0}

    t = truth.loc[valid].values.astype(float)
    p = predicted.loc[valid].values.astype(float)
    errors = p - t

    mae = float(np.abs(errors).mean())
    rmse = float(np.sqrt((errors ** 2).mean()))
    bias = float(errors.mean())

    # R² (coefficient of determination)
    ss_res = float(((t - p) ** 2).sum())
    ss_tot = float(((t - t.mean()) ** 2).sum())
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Median absolute error
    med_ae = float(np.median(np.abs(errors)))

    # 90th percentile error
    p90 = float(np.percentile(np.abs(errors), 90))

    return {
        "label": label,
        "n": n,
        "mae": mae,
        "rmse": rmse,
        "bias": bias,
        "r2": r2,
        "med_ae": med_ae,
        "p90": p90,
    }


def vp_continuity_rh(
    donor_t: pd.Series,
    donor_rh: pd.Series,
    target_t: pd.Series,
) -> pd.Series:
    """Compute RH at target using vapor pressure continuity from donor.

    e_donor = actual_vp(donor_t, donor_rh)
    rh_target = rh_from_vp(target_t, e_donor)
    """
    from pea_met_network.vapor_pressure import (
        actual_vapor_pressure,
        rh_from_vapor_pressure,
    )

    # Both donor and target must have valid data
    valid = donor_t.notna() & donor_rh.notna() & target_t.notna()
    result = pd.Series(np.nan, index=donor_t.index, dtype=float)

    if valid.sum() == 0:
        return result

    dt = donor_t.loc[valid].values.astype(float)
    drh = donor_rh.loc[valid].values.astype(float)
    tt = target_t.loc[valid].values.astype(float)

    e = actual_vapor_pressure(dt, drh)
    rh = rh_from_vapor_pressure(tt, e)

    result.loc[valid] = rh
    return result.clip(0, 100)


def main():
    print("=" * 70)
    print("HISTORICAL RH IMPUTATION VALIDATION")
    print("  Target: Greenwich (pretend RH sensor missing)")
    print("  Source: Processed hourly CSVs (~2 years)")
    print("=" * 70)

    # Load data
    print("\nLoading station data...")
    greenwich = load_station("greenwich")
    cavendish = load_station("cavendish")
    north_rustico = load_station("north_rustico")

    print(f"  Greenwich:    {len(greenwich)} rows, {greenwich.index[0]} → {greenwich.index[-1]}")
    print(f"  Cavendish:    {len(cavendish)} rows, {cavendish.index[0]} → {cavendish.index[-1]}")
    print(f"  N. Rustico:   {len(north_rustico)} rows, {north_rustico.index[0]} → {north_rustico.index[-1]}")

    # Truth: Greenwich observed RH
    gw_rh = greenwich["relative_humidity_pct"]
    gw_t = greenwich["air_temperature_c"]

    # Donor columns
    cav_t = cavendish["air_temperature_c"]
    cav_rh = cavendish["relative_humidity_pct"]
    nr_rh = north_rustico["relative_humidity_pct"]

    # Align all series to common index (stations may differ by 1 row)
    common_idx = gw_rh.index.intersection(cav_rh.index).intersection(nr_rh.index)
    gw_rh = gw_rh.loc[common_idx]
    gw_t = gw_t.loc[common_idx]
    cav_t = cav_t.loc[common_idx]
    cav_rh = cav_rh.loc[common_idx]
    nr_rh = nr_rh.loc[common_idx]
    print(f"  Common index: {len(common_idx)} hours")

    print(f"\n  Greenwich RH coverage: {gw_rh.notna().sum()} / {len(gw_rh)} hours "
          f"({100 * gw_rh.notna().mean():.1f}%)")

    if gw_rh.notna().sum() < 1000:
        print("ERROR: Not enough truth data for meaningful validation")
        sys.exit(1)

    # --- Method 1: VP continuity from Cavendish ---
    print("\n--- Method 1: VP Continuity (Cavendish T+RH → Greenwich T) ---")
    vp_rh = vp_continuity_rh(cav_t, cav_rh, gw_t)

    # --- Method 2: Direct RH from Cavendish ---
    print("--- Method 2: Direct RH Donation (Cavendish RH) ---")
    direct_cav_rh = cav_rh.copy()

    # --- Method 3: Direct RH from North Rustico ---
    print("--- Method 3: Direct RH Donation (N. Rustico RH) ---")
    direct_nr_rh = nr_rh.copy()

    # --- Method 4: Mean of Cavendish + N. Rustico ---
    print("--- Method 4: Mean of Cavendish + N. Rustico RH ---")
    mean_rh = pd.concat([cav_rh, nr_rh], axis=1).mean(axis=1)

    # --- Method 5: Temperature-ratio adjusted Cavendish RH ---
    print("--- Method 5: T-ratio adjusted Cavendish RH ---")
    # RH scales inversely with saturation VP; approximate:
    # rh_target ≈ rh_donor × es(donor_t) / es(target_t)
    from pea_met_network.vapor_pressure import saturation_vapor_pressure
    valid_ratio = cav_rh.notna() & cav_t.notna() & gw_t.notna()
    tratio_rh = pd.Series(np.nan, index=gw_t.index, dtype=float)
    if valid_ratio.sum() > 0:
        es_cav = saturation_vapor_pressure(cav_t.loc[valid_ratio].values.astype(float))
        es_gw = saturation_vapor_pressure(gw_t.loc[valid_ratio].values.astype(float))
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = np.where(es_gw > 0, es_cav / es_gw, 1.0)
        tratio_rh.loc[valid_ratio] = cav_rh.loc[valid_ratio].values.astype(float) * ratio
        tratio_rh = tratio_rh.clip(0, 100)

    # --- Score overall ---
    print("\n" + "=" * 70)
    print("OVERALL RESULTS")
    print("=" * 70)

    methods = [
        (vp_rh, "VP continuity (Cavendish)"),
        (direct_cav_rh, "Direct RH (Cavendish)"),
        (direct_nr_rh, "Direct RH (N. Rustico)"),
        (mean_rh, "Mean RH (Cav+NR)"),
        (tratio_rh, "T-ratio adjusted (Cavendish)"),
    ]

    results = []
    for pred, label in methods:
        r = score(pred, gw_rh, label)
        results.append(r)

    # Print table
    hdr = f"{'Method':<32s} {'N':>6s} {'MAE':>6s} {'MedAE':>6s} {'P90':>6s} {'RMSE':>6s} {'Bias':>7s} {'R²':>6s}"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<32s} {'--':>6s}")
        else:
            print(f"{r['label']:<32s} {r['n']:>6d} {r['mae']:>5.1f}% {r['med_ae']:>5.1f}% "
                  f"{r['p90']:>5.1f}% {r['rmse']:>5.1f}% {r['bias']:>+6.1f}% {r['r2']:>5.3f}")

    # --- Seasonal breakdown ---
    print("\n" + "=" * 70)
    print("SEASONAL BREAKDOWN (Fire Season: May–Sep vs Off-Season: Oct–Apr)")
    print("=" * 70)

    seasons = [
        ("May–Sep (fire season)", lambda idx: idx.month.isin(range(5, 10))),
        ("Oct–Apr (off-season)", lambda idx: ~idx.month.isin(range(5, 10))),
        ("Summer (Jun–Aug)", lambda idx: idx.month.isin([6, 7, 8])),
        ("Winter (Dec–Feb)", lambda idx: idx.month.isin([12, 1, 2])),
    ]

    for season_name, mask_fn in seasons:
        mask = mask_fn(gw_rh.index)
        season_truth = gw_rh[mask]
        season_results = []
        for pred, label in methods:
            season_pred = pred[mask]
            r = score(season_pred, season_truth, label)
            season_results.append(r)

        n_truth = int(season_truth.notna().sum())
        print(f"\n  {season_name} ({n_truth:,} truth hours)")
        print(f"  {'Method':<32s} {'N':>6s} {'MAE':>6s} {'MedAE':>6s} {'P90':>6s} {'RMSE':>6s} {'Bias':>7s} {'R²':>6s}")
        print("  " + "-" * 68)
        for r in season_results:
            if r["n"] == 0:
                print(f"  {r['label']:<32s} {'--':>6s}")
            else:
                print(f"  {r['label']:<32s} {r['n']:>6d} {r['mae']:>5.1f}% {r['med_ae']:>5.1f}% "
                      f"{r['p90']:>5.1f}% {r['rmse']:>5.1f}% {r['bias']:>+6.1f}% {r['r2']:>5.3f}")

    # --- Temperature-range breakdown ---
    print("\n" + "=" * 70)
    print("TEMPERATURE-RANGE BREAKDOWN")
    print("=" * 70)

    temp_ranges = [
        ("< 0°C (freezing)", lambda t: t < 0),
        ("0–10°C", lambda t: (t >= 0) & (t < 10)),
        ("10–20°C", lambda t: (t >= 10) & (t < 20)),
        ("20–30°C", lambda t: (t >= 20) & (t < 30)),
        ("> 30°C", lambda t: t >= 30),
    ]

    for range_name, mask_fn in temp_ranges:
        t_mask = gw_t.notna() & mask_fn(gw_t)
        range_truth = gw_rh[t_mask]
        range_results = []
        for pred, label in methods:
            range_pred = pred[t_mask]
            r = score(range_pred, range_truth, label)
            range_results.append(r)

        n_truth = int(range_truth.notna().sum())
        if n_truth < 50:
            print(f"\n  {range_name}: skip ({n_truth} hours)")
            continue
        print(f"\n  {range_name} ({n_truth:,} truth hours)")
        print(f"  {'Method':<32s} {'N':>6s} {'MAE':>6s} {'MedAE':>6s} {'P90':>6s} {'RMSE':>6s} {'Bias':>7s} {'R²':>6s}")
        print("  " + "-" * 68)
        for r in range_results:
            if r["n"] == 0:
                print(f"  {r['label']:<32s} {'--':>6s}")
            else:
                print(f"  {r['label']:<32s} {r['n']:>6d} {r['mae']:>5.1f}% {r['med_ae']:>5.1f}% "
                      f"{r['p90']:>5.1f}% {r['rmse']:>5.1f}% {r['bias']:>+6.1f}% {r['r2']:>5.3f}")

    # --- Error distribution for best method ---
    scored = [r for r in results if r["n"] > 0]
    if scored:
        best = min(scored, key=lambda r: r["mae"])
        print(f"\n{'=' * 70}")
        print(f"  Best method by MAE: {best['label']} ({best['mae']:.2f}%, R²={best['r2']:.3f})")

        # Error distribution for best method
        best_idx = results.index(best)
        best_pred = methods[best_idx][0]
        valid = gw_rh.notna() & best_pred.notna()
        errors = (best_pred[valid] - gw_rh[valid]).values.astype(float)
        abs_errors = np.abs(errors)

        print(f"\n  Error distribution for '{best['label']}':")
        for pct in [50, 75, 90, 95, 99]:
            val = np.percentile(abs_errors, pct)
            print(f"    {pct}th percentile: {val:.1f}%")
        print(f"    Max error: {np.max(abs_errors):.1f}%")
        print(f"    Hours with error > 10%: {np.sum(abs_errors > 10):,} "
              f"({100 * np.mean(abs_errors > 10):.1f}%)")
        print(f"    Hours with error > 20%: {np.sum(abs_errors > 20):,} "
              f"({100 * np.mean(abs_errors > 20):.1f}%)")

    # ===================================================================
    # FWI-AWARE SCORING — what actually matters for fire weather
    # ===================================================================
    print(f"\n{'=' * 70}")
    print("FWI-AWARE SCORING")
    print("  Compute hourly FFMC chains using each RH method, then derive")
    print("  ISI/BUI/FWI. Compare against truth chain from actual Greenwich obs.")
    print("  Focus: fire season (May-Sep) hours where truth FWI > 0.")
    print(f"{'=' * 70}")

    from pea_met_network.fwi import (
        hourly_fine_fuel_moisture_code as hffmc,
        initial_spread_index as isi_calc,
        buildup_index as bui_calc,
        fire_weather_index as fwi_calc,
    )

    # Greenwich station coords (approx)
    GW_LAT = 46.45

    # Need: temp, rh, wind, rain for each method
    gw_wind = greenwich["wind_speed_kmh"]
    gw_rain = greenwich["rain_mm"]
    gw_temp_aligned = gw_t  # already on common_idx
    gw_wind_aligned = gw_wind.reindex(common_idx)
    gw_rain_aligned = gw_rain.reindex(common_idx)

    # Fire season mask
    fire_season_mask = common_idx.month.isin(range(5, 10))

    def run_fwi_chain(rh_series: pd.Series) -> pd.DataFrame:
        """Run hourly FFMC chain + daily DMC/DC → ISI/BUI/FWI.

        Uses actual Greenwich temp, wind, rain. Only RH differs.
        Returns DataFrame with ffmc, isi, bui, fwi columns.
        """
        rh_aligned = rh_series.reindex(common_idx)

        # Initialize arrays
        n = len(common_idx)
        ffmc_arr = np.full(n, np.nan)
        dmc_arr = np.full(n, np.nan)
        dc_arr = np.full(n, np.nan)
        isi_arr = np.full(n, np.nan)
        bui_arr = np.full(n, np.nan)
        fwi_arr = np.full(n, np.nan)

        # Startup defaults
        ffmc_val = 85.0
        dmc_val = 6.0
        dc_val = 15.0
        last_date = None

        for i, ts in enumerate(common_idx):
            t = gw_temp_aligned.iloc[i]
            rh = rh_aligned.iloc[i]
            w = gw_wind_aligned.iloc[i]
            r = gw_rain_aligned.iloc[i]

            if pd.isna(t) or pd.isna(rh) or pd.isna(w) or pd.isna(r):
                ffmc_arr[i] = ffmc_val
                dmc_arr[i] = dmc_val
                dc_arr[i] = dc_val
                continue

            # Hourly FFMC
            try:
                ffmc_val = hffmc(float(t), float(rh), float(w), float(r), ffmc_val)
                ffmc_arr[i] = ffmc_val
            except Exception:
                ffmc_arr[i] = ffmc_val

            # Daily aggregates for DMC/DC (at 12:00 UTC = ~8am ADT)
            # Use 14:00 UTC for noon solar (more standard for Canadian FWI)
            current_date = ts.date()
            is_noon = ts.hour == 14

            if is_noon or last_date is None or current_date != last_date:
                if is_noon and not pd.isna(rh):
                    try:
                        dmc_val = duff_moisture_code(
                            float(t), float(rh), float(r),
                            dmc_val, ts.month, GW_LAT,
                        )
                    except Exception:
                        pass
                    try:
                        dc_val = drought_code(
                            float(t), float(r), dc_val,
                            ts.month, GW_LAT, float(rh),
                        )
                    except Exception:
                        pass

                if is_noon:
                    isi_arr[i] = isi_calc(ffmc_val, float(w))
                    bui_arr[i] = bui_calc(dmc_val, dc_val)
                    fwi_arr[i] = fwi_calc(isi_arr[i], bui_arr[i])
                last_date = current_date

        return pd.DataFrame({
            "ffmc": ffmc_arr,
            "dmc": dmc_arr,
            "dc": dc_arr,
            "isi": isi_arr,
            "bui": bui_arr,
            "fwi": fwi_arr,
        }, index=common_idx)

    def duff_moisture_code(temp, rh, rain, dmc0, month, lat):
        from pea_met_network.fwi import duff_moisture_code as dmc_fn
        return dmc_fn(temp, rh, rain, dmc0, month, lat)

    def drought_code(temp, rain, dc0, month, lat, rh):
        from pea_met_network.fwi import drought_code as dc_fn
        return dc_fn(temp, rain, dc0, month, lat, rh)

    # Build RH series for each method
    method_rh_series = []
    for pred, label in methods:
        method_rh_series.append((pred, label))

    # Run truth chain first
    print("\n  Computing truth FWI chain (Greenwich observed RH)...")
    truth_fwi = run_fwi_chain(gw_rh)

    # Run chains for each method
    method_fwi_results = []
    for pred, label in method_rh_series:
        print(f"  Computing FWI chain: {label}...")
        method_fwi = run_fwi_chain(pred)
        method_fwi_results.append((method_fwi, label))

    # --- Score FWI components ---
    # Filter to fire season hours where truth FWI > 0 (meaningful fire danger)
    fwi_mask = fire_season_mask & truth_fwi["fwi"].notna() & (truth_fwi["fwi"] > 0)
    n_fwi_hours = int(fwi_mask.sum())
    print(f"\n  Fire season hours with FWI > 0: {n_fwi_hours:,}")

    # Also score all fire season hours (not just FWI > 0)
    fwi_any_mask = fire_season_mask & truth_fwi["fwi"].notna()
    n_fwi_any = int(fwi_any_mask.sum())
    print(f"  All fire season hours with computed FWI: {n_fwi_any:,}")

    # Also look at FFMC (the RH-sensitive hourly component)
    ffmc_fire_mask = fire_season_mask & truth_fwi["ffmc"].notna()
    n_ffmc = int(ffmc_fire_mask.sum())
    print(f"  Fire season hours with FFMC computed: {n_ffmc:,}")

    # Score by component
    for component in ["ffmc", "fwi"]:
        print(f"\n  --- {component.upper()} Error (fire season, FWI>0 hours) ---")
        print(f"  {'Method':<32s} {'N':>6s} {'MAE':>7s} {'MedAE':>7s} {'P90':>7s} {'RMSE':>7s} {'Bias':>8s}")
        print("  " + "-" * 72)

        for (method_fwi, label), _ in zip(method_fwi_results, methods):
            mask = fwi_mask & method_fwi[component].notna() & truth_fwi[component].notna()
            n = int(mask.sum())
            if n == 0:
                print(f"  {label:<32s} {'--':>6s}")
                continue

            t_vals = truth_fwi.loc[mask, component].values.astype(float)
            p_vals = method_fwi.loc[mask, component].values.astype(float)
            errors = p_vals - t_vals

            mae = float(np.abs(errors).mean())
            med_ae = float(np.median(np.abs(errors)))
            p90 = float(np.percentile(np.abs(errors), 90))
            rmse = float(np.sqrt((errors ** 2).mean()))
            bias = float(errors.mean())

            print(f"  {label:<32s} {n:>6d} {mae:>6.2f} {med_ae:>6.2f} {p90:>6.2f} {rmse:>6.2f} {bias:>+7.2f}")

    # --- FFMC error for ALL fire season hours ---
    print(f"\n  --- FFMC Error (ALL fire season hours) ---")
    print(f"  {'Method':<32s} {'N':>6s} {'MAE':>7s} {'MedAE':>7s} {'P90':>7s} {'RMSE':>7s} {'Bias':>8s}")
    print("  " + "-" * 72)

    for (method_fwi, label), _ in zip(method_fwi_results, methods):
        mask = ffmc_fire_mask & method_fwi["ffmc"].notna() & truth_fwi["ffmc"].notna()
        n = int(mask.sum())
        if n == 0:
            print(f"  {label:<32s} {'--':>6s}")
            continue

        t_vals = truth_fwi.loc[mask, "ffmc"].values.astype(float)
        p_vals = method_fwi.loc[mask, "ffmc"].values.astype(float)
        errors = p_vals - t_vals

        mae = float(np.abs(errors).mean())
        med_ae = float(np.median(np.abs(errors)))
        p90 = float(np.percentile(np.abs(errors), 90))
        rmse = float(np.sqrt((errors ** 2).mean()))
        bias = float(errors.mean())

        print(f"  {label:<32s} {n:>6d} {mae:>6.2f} {med_ae:>6.2f} {p90:>6.2f} {rmse:>6.2f} {bias:>+7.2f}")

    # --- FWI rating accuracy ---
    # CFFDRS FWI rating classes: Low 0-5, Moderate 5-14, High 14-21, Very High 21-33, Extreme >33
    print(f"\n  --- FWI Rating Class Agreement (fire season, FWI>0) ---")
    print(f"  {'Method':<32s} {'N':>6s} {'Exact':>6s} {'±1':>6s} {'±2':>6s}")
    print("  " + "-" * 58)

    def fwi_class(fwi_val):
        if fwi_val <= 5: return 0
        if fwi_val <= 14: return 1
        if fwi_val <= 21: return 2
        if fwi_val <= 33: return 3
        return 4

    class_names = ["Low", "Mod", "High", "V.High", "Extreme"]

    for (method_fwi, label), _ in zip(method_fwi_results, methods):
        mask = fwi_mask & method_fwi["fwi"].notna() & truth_fwi["fwi"].notna()
        n = int(mask.sum())
        if n == 0:
            print(f"  {label:<32s} {'--':>6s}")
            continue

        t_classes = truth_fwi.loc[mask, "fwi"].apply(fwi_class).values
        p_classes = method_fwi.loc[mask, "fwi"].apply(fwi_class).values

        exact = int(np.sum(t_classes == p_classes))
        within1 = int(np.sum(np.abs(t_classes - p_classes) <= 1))
        within2 = int(np.sum(np.abs(t_classes - p_classes) <= 2))

        print(f"  {label:<32s} {n:>6d} {100*exact/n:>5.1f}% {100*within1/n:>5.1f}% {100*within2/n:>5.1f}%")

    # --- High-danger hours: FWI > 14 (High+) ---
    high_danger_mask = fire_season_mask & truth_fwi["fwi"].notna() & (truth_fwi["fwi"] > 14)
    n_high = int(high_danger_mask.sum())
    if n_high > 0:
        print(f"\n  --- FFMC Error (fire season, FWI > 14 = High/Very High/Extreme, {n_high} hours) ---")
        print(f"  {'Method':<32s} {'N':>6s} {'MAE':>7s} {'MedAE':>7s} {'P90':>7s} {'RMSE':>7s} {'Bias':>8s}")
        print("  " + "-" * 72)

        for (method_fwi, label), _ in zip(method_fwi_results, methods):
            mask = high_danger_mask & method_fwi["ffmc"].notna() & truth_fwi["ffmc"].notna()
            n = int(mask.sum())
            if n == 0:
                print(f"  {label:<32s} {'--':>6s}")
                continue

            t_vals = truth_fwi.loc[mask, "ffmc"].values.astype(float)
            p_vals = method_fwi.loc[mask, "ffmc"].values.astype(float)
            errors = p_vals - t_vals

            mae = float(np.abs(errors).mean())
            med_ae = float(np.median(np.abs(errors)))
            p90 = float(np.percentile(np.abs(errors), 90))
            rmse = float(np.sqrt((errors ** 2).mean()))
            bias = float(errors.mean())

            print(f"  {label:<32s} {n:>6d} {mae:>6.2f} {med_ae:>6.2f} {p90:>6.2f} {rmse:>6.2f} {bias:>+7.2f}")

    print()


if __name__ == "__main__":
    main()
