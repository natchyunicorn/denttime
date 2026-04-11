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

    # ---- Post-filter: enforce ML + privacy constraints ----
    before = len(df_out)

    # 1. Drop rows without dentist — DentTime needs dentist_pseudo_id as feature
    df_out = df_out.dropna(subset=["dentist_pseudo_id"])
    dropped_no_dentist = before - len(df_out)
    if dropped_no_dentist > 0:
        print(f"\nDropped {dropped_no_dentist} rows with missing dentist_pseudo_id")

    # 2. Drop rows violating k-anonymity (k=5)
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
