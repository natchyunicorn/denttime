# DentTime Data Pipeline

โปรเจกต์ data pipeline สำหรับ DentTime — ML ทำนายระยะเวลาหัตถการทันตกรรม

**Scope:** Data Ingestion → Validation → Anonymization → Handoff (ส่ง clean CSV ให้ทีม Feature Engineering)

---

## โครงสร้างโปรเจกต์

```
denttime_data/
├── anonymize_for_ml.py              # Script anonymize (HMAC + k-anonymity)
├── validate_output.py               # Quality filter (Phase 1)
├── README.md
├── HANDOFF.md
├── .gitignore
├── airflow_home/
│   ├── dags/
│   │   └── denttime_pipeline.py     # DAG หลัก (Phase 2)
│   ├── airflow.cfg                  # Airflow config (auto-generated)
│   └── airflow.db                   # SQLite metadata (auto-generated)
└── data/
    ├── raw/                         # CSV ดิบ (มี PII — ห้ามเปิด/commit)
    ├── incoming/                    # วาง CSV ใหม่ที่นี่ → Airflow หยิบไปประมวลผล
    ├── staging/                     # ไฟล์ระหว่างทาง (ingested, anonymized)
    ├── output/                      # ผลลัพธ์ Phase 1
    └── published/                   # ผลลัพธ์สุดท้าย ส่งมอบให้เพื่อน
        └── {ds}/
            ├── data.csv             # Clean CSV พร้อมใช้
            └── manifest.json        # Metadata (row_count, source_files)
```

---

## Phase 1: Manual Pipeline

รัน script ด้วยมือทีละขั้น:

```bash
# 1. Anonymize: อ่าน raw CSV → output
python3 anonymize_for_ml.py data/output/output.csv data/raw/*.csv

# 2. Validate: quality filter → output_clean
python3 validate_output.py data/output/output.csv data/output/output_clean.csv
```

---

## Phase 2: Automated Pipeline (Airflow)

ทุกเดือนเมื่อมี CSV ใหม่วางใน `data/incoming/` → Airflow หยิบไปประมวลผลอัตโนมัติ

### Pipeline Diagram

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│   ingest    │───▶│   validate   │───▶│  anonymize   │───▶│   publish    │
│             │    │              │    │              │    │              │
│ • หา CSV    │    │ • Schema     │    │ • HMAC       │    │ • Copy ไป    │
│   ใน        │    │   ครบ 14 col │    │   pseudonym   │    │   published/ │
│   incoming/ │    │ • Volume     │    │ • k-anonymity │    │ • สร้าง      │
│ • รวมไฟล์   │    │   >= 1000    │    │ • Drop PII   │    │   manifest   │
│ • Dedupe    │    │ • Duration   │    │              │    │   .json      │
│             │    │   5-480 min  │    │              │    │              │
│             │    │ • Amount >= 0│    │              │    │              │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘

Scope coverage:
  Ingestion   → ingest task
  Validation  → validate task
  Reliability → retries=2, retry_delay=1min, DAG fail on bad data
```

### Scope ครอบคลุม 3 ด้าน

| Scope | จัดการที่ Task | รายละเอียด |
|-------|----------------|------------|
| **Data Ingestion** | `ingest` | รวม CSV หลายไฟล์, deduplicate บน appointment_id |
| **Validation** | `validate` | เช็ค schema, volume, business rules — fail เมื่อข้อมูลไม่ผ่าน |
| **Reliability** | ทุก task | retries=2, retry_delay=1 นาที, DAG หยุดเมื่อ validate fail |

### ความต้องการ

- Python 3.13+
- Apache Airflow 3.2.0

### ขั้นตอนติดตั้ง (ครั้งแรกเท่านั้น)

```bash
# 1. ติดตั้ง Airflow
pip install "apache-airflow==3.2.0"

# 2. ตั้ง AIRFLOW_HOME ชี้ไปที่โฟลเดอร์ในโปรเจกต์
#    (ใส่ใน .zshrc หรือ .bashrc ได้ถ้าไม่อยากพิมพ์ทุกครั้ง)
export AIRFLOW_HOME=~/Desktop/denttime_data/airflow_home

# 3. สร้าง database เริ่มต้น
airflow db migrate
```

### วิธีเปิด Airflow (ทุกครั้งที่ใช้งาน)

```bash
# ตั้ง AIRFLOW_HOME ก่อน (ถ้ายังไม่ได้ใส่ใน .zshrc)
export AIRFLOW_HOME=~/Desktop/denttime_data/airflow_home

# รันคำสั่ง standalone — เปิด scheduler + webserver + triggerer ในคำสั่งเดียว
airflow standalone
```

หลังรันจะเห็น log วิ่งใน terminal:
- **Web UI:** เปิด http://localhost:8080
- **Username:** `admin`
- **Password:** ดูจากไฟล์ `airflow_home/simple_auth_manager_passwords.json.generated`

กด `Ctrl+C` เพื่อหยุด Airflow

### วิธี Trigger DAG

**ผ่าน Web UI:**
1. เปิด http://localhost:8080
2. หา DAG ชื่อ `denttime_pipeline`
3. กดปุ่ม Trigger (▶)

**ผ่าน command line:**
```bash
export AIRFLOW_HOME=~/Desktop/denttime_data/airflow_home

# วาง CSV ใหม่ใน incoming/ ก่อน
cp new_data.csv data/incoming/

# Trigger DAG
airflow dags trigger denttime_pipeline
```

### ทดสอบ DAG โดยไม่ต้องเปิด Airflow

```bash
export AIRFLOW_HOME=~/Desktop/denttime_data/airflow_home

# รัน DAG แบบ test (ไม่บันทึกลง database จริง)
airflow dags test denttime_pipeline 2026-04-01
```

### Troubleshooting

| ปัญหา | วิธีแก้ |
|--------|---------|
| `command not found: airflow` | ยังไม่ได้ `pip install` หรือ venv ไม่ตรง |
| DAG ไม่ขึ้นใน Web UI | เช็คว่า `AIRFLOW_HOME` ชี้ถูกที่ และมีไฟล์ DAG อยู่ใน `airflow_home/dags/` |
| Port 8080 ถูกใช้อยู่ | ปิดโปรแกรมอื่นที่ใช้ port 8080 หรือแก้ port ใน `airflow.cfg` |
| anonymize fail: `No module named 'pandas'` | `pip install pandas` ใน environment เดียวกับ Airflow |
