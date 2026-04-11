"""
DentTime Monthly Pipeline DAG

ทำอะไร: ทุกเดือนหยิบ CSV ใหม่จาก data/incoming/ → ตรวจสอบ → anonymize → ส่งให้เพื่อน
Flow:  ingest → validate → anonymize → publish

วิธีทดสอบ:
    export AIRFLOW_HOME=~/Desktop/denttime_data/airflow_home
    airflow dags test denttime_pipeline 2026-04-01
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, task

# =============================================================
# Path Configuration
# =============================================================
# PROJECT_ROOT คือ root ของโปรเจกต์ denttime_data/
# DAG file อยู่ที่ airflow_home/dags/ → ขึ้นไป 2 ระดับจะถึง root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# โฟลเดอร์ต่าง ๆ ที่ pipeline ใช้
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"     # CSV ใหม่วางที่นี่
STAGING_DIR = PROJECT_ROOT / "data" / "staging"       # ไฟล์ระหว่างทาง
PUBLISHED_DIR = PROJECT_ROOT / "data" / "published"   # ผลลัพธ์ส่งมอบเพื่อน
ANONYMIZE_SCRIPT = PROJECT_ROOT / "anonymize_for_ml.py"

# =============================================================
# Columns ที่คาดว่า raw CSV ต้องมี
# (ดูจาก anonymize_for_ml.py ว่าต้องการ column อะไรบ้าง)
# =============================================================
EXPECTED_COLUMNS = [
    "clinic_name",
    "branch_id",
    "license_no",
    "appointment_id",
    "treatment",
    "tooth_no",
    "surfaces",
    "total_amount",
    "notes",
    "appointment_start",
    "appointment_end",
    "check_in_time",
    "treatment_record_time",
    "receipt_time",
]

# =============================================================
# DAG Definition
# =============================================================
# default_args ใช้กับทุก task ใน DAG นี้
default_args = {
    "retries": 2,                           # ถ้า task fail จะ retry ได้ 2 ครั้ง
    "retry_delay": timedelta(minutes=1),    # รอ 1 นาทีก่อน retry
}

# สร้าง DAG object
# - schedule="@monthly"  = รันเดือนละครั้ง
# - catchup=False         = ไม่ย้อนรัน DAG ที่พลาดไป
# - ds (execution date)   = Airflow ส่งให้อัตโนมัติ เช่น "2026-04-01"
with DAG(
    dag_id="denttime_pipeline",
    description="Monthly DentTime data pipeline: ingest → validate → anonymize → publish",
    default_args=default_args,
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["denttime", "data-pipeline"],
) as dag:

    # =========================================================
    # Task 1: INGEST
    # =========================================================
    # หา CSV ใหม่ใน data/incoming/ → รวมไฟล์ → dedupe → save
    @task
    def ingest(ds: str) -> dict:
        """
        หยิบ CSV ทั้งหมดจาก data/incoming/
        - ถ้าไม่มีไฟล์ → skip (return row_count=0)
        - รวมไฟล์ทั้งหมดเป็น DataFrame เดียว
        - Deduplicate บน appointment_id
        - Save ไป data/staging/ingested_{ds}.csv
        """
        # หาไฟล์ CSV ทั้งหมดใน incoming/
        csv_files = sorted(glob.glob(str(INCOMING_DIR / "*.csv")))

        if not csv_files:
            print("⚠️ No CSV files found in data/incoming/ — skipping.")
            return {"row_count": 0, "source_files": [], "output_path": ""}

        print(f"Found {len(csv_files)} CSV file(s) in incoming/:")
        for f in csv_files:
            print(f"  - {os.path.basename(f)}")

        # อ่านทุกไฟล์แล้วรวมกัน
        frames = []
        for f in csv_files:
            df = pd.read_csv(f)
            print(f"  Loaded {len(df):>6} rows from {os.path.basename(f)}")
            frames.append(df)

        combined = pd.concat(frames, ignore_index=True)
        print(f"\nTotal rows after concat: {len(combined)}")

        # Deduplicate — appointment เดียวกันอาจอยู่หลายไฟล์
        before = len(combined)
        combined = combined.drop_duplicates(subset=["appointment_id"], keep="last")
        dupes = before - len(combined)
        if dupes > 0:
            print(f"Removed {dupes} duplicate appointment_id rows")

        # สร้าง staging/ ถ้ายังไม่มี แล้ว save
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(STAGING_DIR / f"ingested_{ds}.csv")
        combined.to_csv(output_path, index=False)
        print(f"\n✅ Saved {len(combined)} rows → {output_path}")

        # Return ข้อมูลให้ task ถัดไปใช้ (ผ่าน XCom อัตโนมัติ)
        return {
            "row_count": len(combined),
            "source_files": [os.path.basename(f) for f in csv_files],
            "output_path": output_path,
        }

    # =========================================================
    # Task 2: VALIDATE
    # =========================================================
    # เช็คข้อมูลก่อนส่งต่อ — ถ้า fail จะ raise error ให้ DAG หยุด
    @task
    def validate(ingest_result: dict) -> str:
        """
        ตรวจสอบคุณภาพข้อมูล 3 ด้าน:
        1. Schema  — column ครบตามที่ anonymize script ต้องการ
        2. Volume  — จำนวน row >= 1000 (กัน empty/corrupt file)
        3. Business — duration อยู่ในช่วง 5-480 นาที, total_amount >= 0
        ถ้า fail → raise error → DAG หยุด (Airflow จะ retry ตาม default_args)
        """
        row_count = ingest_result["row_count"]
        output_path = ingest_result["output_path"]

        # ถ้า ingest ไม่มีไฟล์ → skip validate ด้วย
        if row_count == 0:
            print("⚠️ No data to validate (ingest was skipped).")
            return ""

        df = pd.read_csv(output_path)
        errors = []  # เก็บ error ทั้งหมดไว้ แล้วแสดงทีเดียวตอนท้าย

        # --- Check 1: Schema ---
        missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            errors.append(f"SCHEMA FAIL: missing columns: {missing_cols}")
            print(f"❌ Schema: FAIL — missing {missing_cols}")
        else:
            print(f"✅ Schema: PASS — all {len(EXPECTED_COLUMNS)} columns present")

        # --- Check 2: Volume ---
        if len(df) < 1000:
            errors.append(f"VOLUME FAIL: only {len(df)} rows (minimum 1000)")
            print(f"❌ Volume: FAIL — {len(df)} rows (need >= 1000)")
        else:
            print(f"✅ Volume: PASS — {len(df)} rows")

        # --- Check 3: Business Rules ---
        # คำนวณ duration จาก appointment_start/end (เหมือนที่ anonymize script ทำ)
        start = pd.to_datetime(df["appointment_start"], errors="coerce")
        end = pd.to_datetime(df["appointment_end"], errors="coerce")
        duration_min = (end - start).dt.total_seconds() / 60

        # เช็คว่า duration อยู่ในช่วง 5-480 นาที (ที่ไม่ใช่ NaN)
        valid_duration = duration_min.dropna()
        bad_duration = valid_duration[(valid_duration < 5) | (valid_duration > 480)]
        if len(bad_duration) > 0:
            pct = len(bad_duration) / len(valid_duration) * 100
            errors.append(
                f"BUSINESS FAIL: {len(bad_duration)} rows ({pct:.1f}%) "
                f"with duration outside 5-480 min"
            )
            print(f"❌ Business (duration): FAIL — {len(bad_duration)} rows out of range")
        else:
            print(f"✅ Business (duration): PASS — all in 5-480 min range")

        # เช็ค total_amount >= 0
        bad_amount = df[df["total_amount"] < 0]
        if len(bad_amount) > 0:
            errors.append(f"BUSINESS FAIL: {len(bad_amount)} rows with negative total_amount")
            print(f"❌ Business (amount): FAIL — {len(bad_amount)} rows negative")
        else:
            print(f"✅ Business (amount): PASS — no negative amounts")

        # ถ้ามี error ใดก็ตาม → raise ให้ DAG fail
        if errors:
            error_msg = "\n".join(errors)
            print(f"\n{'='*60}")
            print("VALIDATION FAILED:")
            print(error_msg)
            print(f"{'='*60}")
            raise ValueError(f"Validation failed:\n{error_msg}")

        print(f"\n✅ All validation checks passed!")
        return output_path

    # =========================================================
    # Task 3: ANONYMIZE
    # =========================================================
    # เรียก anonymize_for_ml.py ผ่าน subprocess
    @task
    def anonymize(validated_path: str, ds: str) -> str:
        """
        เรียก anonymize_for_ml.py เพื่อทำ:
        - HMAC pseudonymization (clinic, dentist, appointment)
        - Generalize timestamps
        - Drop PII columns
        - k-anonymity filtering

        ใช้ subprocess เรียก script ตรง ๆ ไม่ต้อง refactor
        """
        # ถ้า validate ส่ง empty string มา = ไม่มีข้อมูล → skip
        if not validated_path:
            print("⚠️ No data to anonymize (previous steps were skipped).")
            return ""

        # กำหนด output path
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(STAGING_DIR / f"anonymized_{ds}.csv")

        # เรียก: python3 anonymize_for_ml.py <output.csv> <input.csv>
        # (script รับ output path ก่อน แล้ว input path ตามหลัง)
        # ใช้ sys.executable แทน "python3" เพื่อให้แน่ใจว่าใช้ Python ตัวเดียวกับ Airflow
        cmd = [
            sys.executable,
            str(ANONYMIZE_SCRIPT),
            output_path,         # arg 1: output file
            validated_path,      # arg 2: input file
        ]

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        # แสดง output ของ script
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")

        # ถ้า script fail → raise error
        if result.returncode != 0:
            raise RuntimeError(
                f"anonymize_for_ml.py failed (exit code {result.returncode})"
            )

        # ยืนยันว่าไฟล์ถูกสร้างจริง
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Expected output not found: {output_path}")

        row_count = len(pd.read_csv(output_path))
        print(f"\n✅ Anonymized: {row_count} rows → {output_path}")
        return output_path

    # =========================================================
    # Task 4: PUBLISH
    # =========================================================
    # Copy ไฟล์ไป data/published/{ds}/ + สร้าง manifest
    @task
    def publish(anonymized_path: str, ingest_result: dict, ds: str) -> None:
        """
        ส่งมอบไฟล์ให้เพื่อน:
        1. Copy ไฟล์ไป data/published/{ds}/data.csv
        2. สร้าง manifest.json (metadata ของการรัน)
        """
        # ถ้าไม่มีข้อมูล → skip
        if not anonymized_path:
            print("⚠️ No data to publish (previous steps were skipped).")
            return

        # สร้างโฟลเดอร์ data/published/{ds}/
        publish_dir = PUBLISHED_DIR / ds
        publish_dir.mkdir(parents=True, exist_ok=True)

        # Copy ไฟล์
        dest_path = publish_dir / "data.csv"
        shutil.copy2(anonymized_path, dest_path)

        # นับจำนวน row ในไฟล์ที่ publish
        row_count = len(pd.read_csv(dest_path))

        # สร้าง manifest.json — metadata ง่าย ๆ
        manifest = {
            "created_at": datetime.now().isoformat(),
            "row_count": row_count,
            "source_files": ingest_result.get("source_files", []),
        }

        manifest_path = publish_dir / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        print(f"\n{'='*60}")
        print(f"✅ Ready for handoff: data/published/{ds}/")
        print(f"   data.csv      : {row_count} rows")
        print(f"   manifest.json : {manifest_path}")
        print(f"{'='*60}")

    # =========================================================
    # Wire tasks together: ingest → validate → anonymize → publish
    # =========================================================
    # TaskFlow API ส่งผลลัพธ์จาก task หนึ่งไปอีก task ผ่าน XCom อัตโนมัติ
    ingest_result = ingest()
    validated_path = validate(ingest_result)
    anonymized_path = anonymize(validated_path)
    publish(anonymized_path, ingest_result)
