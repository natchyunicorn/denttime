"""
Anonymize DentCloud appointment CSV for DentTime ML training.

Strategy: Case B (irreversible pseudonymization)
- Generates ephemeral HMAC key per run, discarded on exit.
- Drops direct identifiers.
- HMACs clinic_name (NOT branch — branches in same chain share tier).
- HMACs license_no (dentist) and appointment_id.
- Generalizes timestamps into ML-friendly features.
- Drops free-text notes.

Usage:
    python3 anonymize_for_ml.py input.csv output.csv
"""

import sys
import hmac
import hashlib
import secrets
import pandas as pd

# Ephemeral key — sampled at runtime, never persisted.
EPHEMERAL_KEY = secrets.token_bytes(32)


def h(msg, prefix: str = "") -> str:
    """HMAC-SHA256, truncated to 16 hex chars (64 bits)."""
    if msg is None or (isinstance(msg, float) and pd.isna(msg)) or msg == "":
        return None
    digest = hmac.new(
        EPHEMERAL_KEY,
        str(msg).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()[:16]
    return f"{prefix}{digest}"


def normalize_clinic_name(name):
    """Collapse whitespace + lowercase so chain variants hash to same id."""
    if pd.isna(name):
        return None
    return " ".join(str(name).strip().lower().split())


def normalize_license(x):
    """ท.12345 / ท12345 / 00012345 -> 12345"""
    if pd.isna(x):
        return None
    s = str(x).strip().upper().replace("ท.", "").replace("ท", "")
    return s.lstrip("0") or "0"


def to_minutes(delta):
    if pd.isna(delta):
        return None
    return int(delta.total_seconds() // 60)


def anonymize(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()

    # Pseudonymized IDs
    out["clinic_pseudo_id"] = df["clinic_name"].apply(
        lambda x: h(normalize_clinic_name(x), prefix="C_")
    )
    out["dentist_pseudo_id"] = df["license_no"].apply(
        lambda x: h(normalize_license(x), prefix="D_")
    )
    out["appointment_pseudo_id"] = df["appointment_id"].apply(
        lambda x: h(x, prefix="A_")
    )

    # ADR-001: keep rows with missing dentist license instead of dropping them.
    # has_dentist_id = 1 when license_no exists, 0 when missing.
    # FE team uses this flag for Two-Tier Lookup fallback strategy.
    out["has_dentist_id"] = df["license_no"].notna().astype(int)

    # Clinical features (kept)
    out["treatment"] = df["treatment"]
    out["tooth_no"] = df["tooth_no"]
    out["surfaces"] = df["surfaces"]
    out["total_amount"] = df["total_amount"]

    # Notes presence flag (V2): record IF notes exist, never WHAT they contain.
    # Rationale: doctors writing notes often signals a non-standard case,
    # which may correlate with duration. Zero PII leakage since only 0/1.
    out["has_notes"] = df["notes"].notna().astype(int)

    # Time features
    start = pd.to_datetime(df["appointment_start"], errors="coerce")
    end = pd.to_datetime(df["appointment_end"], errors="coerce")
    checkin = pd.to_datetime(df["check_in_time"], errors="coerce")
    tx_record = pd.to_datetime(df["treatment_record_time"], errors="coerce")
    receipt = pd.to_datetime(df["receipt_time"], errors="coerce")

    out["appt_year_month"] = start.dt.strftime("%Y-%m")
    out["appt_day_of_week"] = start.dt.dayofweek
    out["appt_hour_bucket"] = (start.dt.hour // 4) * 4  # 4h buckets: 0,4,8,12,16,20

    out["scheduled_duration_min"] = (end - start).apply(to_minutes)
    out["checkin_delay_min"] = (checkin - start).apply(to_minutes)
    out["tx_record_offset_min"] = (tx_record - start).apply(to_minutes)
    out["receipt_offset_min"] = (receipt - start).apply(to_minutes)

    out["checked_in"] = checkin.notna().astype(int)
    out["treatment_recorded"] = tx_record.notna().astype(int)
    out["receipt_issued"] = receipt.notna().astype(int)

    # ---- Appointment order features (HANDOFF spec: is_first_case) ----
    # Rank appointments within (dentist, date) by start time.
    # "is_first_case" = dentist's first appointment of the day.
    # This signal is lost by the 4-hour bucket generalization above.
    dentist_key = df["license_no"].apply(normalize_license)
    date_key = start.dt.date

    rank_df = pd.DataFrame({
        "_dentist": dentist_key,
        "_date": date_key,
        "_start": start,
        "_aid": df["appointment_id"],   # tie-breaker for same-minute slots
    }).reset_index()

    # Sort by dentist → date → start time → appointment_id (deterministic)
    rank_df = rank_df.sort_values(["_dentist", "_date", "_start", "_aid"])

    # dropna=False: keep null-license rows in their own group
    # (without this, pandas skips null keys → length mismatch on assign)
    rank_df["_rank"] = (
        rank_df.groupby(["_dentist", "_date"], dropna=False).cumcount() + 1
    )

    # Restore original row order before assigning back
    rank_df = rank_df.sort_values("index")

    out["appointment_rank_in_day"] = rank_df["_rank"].values
    out["is_first_case"] = (out["appointment_rank_in_day"] == 1).astype(int)

    # Null-license handling: rank within "unknown dentist" partition has no
    # semantic meaning (it groups ALL unknown dentists together).
    # Overwrite with NaN/0 so the feature doesn't leak false signal.
    null_license_mask = df["license_no"].isna().values
    out.loc[null_license_mask, "appointment_rank_in_day"] = pd.NA
    out.loc[null_license_mask, "is_first_case"] = 0

    return out


def k_anonymity_check(df, k=5):
    qi = ["clinic_pseudo_id", "appt_year_month", "appt_day_of_week", "appt_hour_bucket"]
    counts = df.groupby(qi, dropna=False).size()
    violators = counts[counts < k]
    total = len(df)
    bad = int(violators.sum())
    pct = (bad / total * 100) if total else 0

    print(f"\nk-anonymity check (k={k}):")
    print(f"  Total QI groups       : {len(counts)}")
    print(f"  Groups violating k={k} : {len(violators)}")
    print(f"  Rows in violating grps: {bad}  ({pct:.2f}% of total)")
    if bad > 0:
        if pct < 1:
            print("  ✅ Below 1% — acceptable for ML training.")
        elif pct < 5:
            print("  ⚠️  1–5% — review before sharing outside team.")
        else:
            print("  ❌ >5% — consider coarsening features further.")


def pre_check(df):
    print("=" * 60)
    print("PRE-CHECK")
    print("=" * 60)
    print(f"Total rows              : {len(df)}")
    print(f"Unique clinic_name      : {df['clinic_name'].nunique()}")
    print(f"Unique branch_id        : {df['branch_id'].nunique()}")
    print(f"Unique license_no       : {df['license_no'].nunique()}")

    bpc = df.groupby("clinic_name")["branch_id"].nunique()
    print(f"\nBranches per clinic_name:")
    print(f"  min={bpc.min()}, median={int(bpc.median())}, max={bpc.max()}")

    for col in ["clinic_name", "license_no", "appointment_start", "treatment"]:
        n = df[col].isna().sum()
        if n > 0:
            print(f"  ⚠️  {col}: {n} missing values")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 anonymize_for_ml.py <output.csv> <input1.csv> [input2.csv ...]")
        print("Example: python3 anonymize_for_ml.py output.csv *.csv")
        sys.exit(1)

    out_path = sys.argv[1]
    in_paths = sys.argv[2:]

    # Load and concatenate ALL input files so we anonymize with ONE key.
    # This ensures the same clinic/dentist gets the same pseudo_id across files.
    frames = []
    for p in in_paths:
        d = pd.read_csv(p)
        print(f"Loaded {len(d):>6} rows from {p}")
        d["_source_file"] = p  # keep track of origin (dropped before output)
        frames.append(d)
    df_in = pd.concat(frames, ignore_index=True)
    print(f"\nTotal merged: {len(df_in)} rows from {len(in_paths)} files\n")

    # Deduplicate on appointment_id — same appointment may appear in overlapping exports
    before = len(df_in)
    df_in = df_in.drop_duplicates(subset=["appointment_id"], keep="last")
    dupes = before - len(df_in)
    if dupes > 0:
        print(f"Removed {dupes} duplicate appointment_id rows (kept most recent)\n")

    df_in = df_in.drop(columns=["_source_file"])

    pre_check(df_in)

    df_out = anonymize(df_in)

    # ---- Post-filter: enforce privacy constraints ----
    # ADR-001: no longer drop rows with missing dentist_pseudo_id.
    # Rows are kept with dentist_pseudo_id=NULL + has_dentist_id=0.
    null_dentist = df_out["dentist_pseudo_id"].isna().sum()
    print(f"\nRows with missing dentist_pseudo_id (kept): {null_dentist}")

    # Drop rows violating k-anonymity (k=5)
    qi = ["clinic_pseudo_id", "appt_year_month", "appt_day_of_week", "appt_hour_bucket"]
    group_sizes = df_out.groupby(qi, dropna=False).transform("size")
    before_k = len(df_out)
    df_out = df_out[group_sizes >= 5]
    dropped_k = before_k - len(df_out)
    if dropped_k > 0:
        print(f"Dropped {dropped_k} rows violating k-anonymity (k=5)")

    df_out.to_csv(out_path, index=False)
    print(f"\nWrote {len(df_out)} rows to {out_path}")
    print(f"Output columns: {list(df_out.columns)}")

    k_anonymity_check(df_out, k=5)
    print("\n✅ Ephemeral key discarded. Pseudonymization is irreversible.")
    print("   Next: use output.csv to train DentTime. Delete after training.")
