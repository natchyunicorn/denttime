# DentTime — Handoff Document

**จาก:** Natcha (Data Ingestion + Validation + Reliability)
**ถึง:** ทีม Feature Engineering + Model Registry
**วันที่:** 11 เมษายน 2026

---

## สิ่งที่ทำแล้ว + สิ่งที่ยังไม่ได้ทำ

```
Raw CSV (PII)
    │
    ▼
┌──────────────────────────────────────────────────────┐
│  ✅ ทำแล้ว (Natcha)                                  │
│                                                      │
│  ingest    → รวมไฟล์ + dedupe (appointment_id)       │
│  validate  → เช็ค schema / volume / business rules   │
│  anonymize → HMAC pseudonym + k-anonymity (k=5)      │
│  publish   → clean CSV + manifest.json               │
└──────────────────────────────────────────────────────┘
    │
    ▼  data/published/{ds}/data.csv  ← จุดส่งมอบ
    │
┌──────────────────────────────────────────────────────┐
│  🔲 ยังไม่ได้ทำ (ทีม FE + Model Registry)           │
│                                                      │
│  Feature Engineering                                 │
│  Train / Test split                                  │
│  Model training + evaluation                         │
│  Model Registry                                      │
└──────────────────────────────────────────────────────┘
```

---

## ไฟล์ที่ส่งมอบ

| ไฟล์ | ที่อยู่ |
|------|---------|
| **Clean CSV** | `data/published/2026-04-01/data.csv` |
| **Metadata** | `data/published/2026-04-01/manifest.json` |

**หมายเหตุ:** ไฟล์ data ไม่ได้อยู่บน GitHub (กัน PII ด้วย .gitignore) — ต้อง copy จากเครื่อง Natcha หรือรัน pipeline เอง

### วิธีโหลด

```python
import pandas as pd

df = pd.read_csv("data/published/2026-04-01/data.csv")
print(df.shape)   # (120534, 18)
```

---

## Schema (18 columns)

### Identifiers (pseudo — ไม่สามารถ reverse กลับเป็นตัวจริงได้)

| Column | Type | ตัวอย่าง | คำอธิบาย |
|--------|------|----------|----------|
| `clinic_pseudo_id` | string | `C_a3f8b2e901c4d7e6` | รหัสคลินิก (HMAC) |
| `dentist_pseudo_id` | string | `D_7b2c9e4f01a8d3b5` | รหัสทันตแพทย์ (HMAC) |
| `appointment_pseudo_id` | string | `A_e5d1f8a2b3c4067e` | รหัสนัดหมาย (HMAC) — unique ทุก row |

### Clinical features

| Column | Type | Nulls | คำอธิบาย |
|--------|------|-------|----------|
| `treatment` | string | 899 (0.7%) | ชื่อหัตถการ เช่น "ขูดหินปูน", "ถอนฟันแท้" |
| `tooth_no` | string | 81,039 (67%) | ซี่ฟัน — null เยอะเพราะหัตถการบางอย่างไม่ระบุซี่ |
| `surfaces` | string | 113,029 (94%) | ด้านฟัน — null เยอะเพราะใช้กับงานอุดเท่านั้น |
| `total_amount` | float | 0 | ค่าบริการ (บาท) — ≥ 0 เสมอ |
| `has_notes` | int | 0 | หมอเขียน notes หรือเปล่า (0/1) — ไม่มีเนื้อหา notes |

### Time features

| Column | Type | Nulls | คำอธิบาย |
|--------|------|-------|----------|
| `appt_year_month` | string | 0 | เดือนของนัด เช่น "2025-01" (range: 2025-01 → 2025-04) |
| `appt_day_of_week` | int | 0 | วันในสัปดาห์ (0=จันทร์ ... 6=อาทิตย์) |
| `appt_hour_bucket` | int | 0 | ช่วงเวลา 4 ชม. (0, 4, 8, 12, 16, 20) |
| `scheduled_duration_min` | int | 0 | **⭐ Target variable** — ระยะเวลานัดที่กำหนด (นาที) |
| `checkin_delay_min` | float | 15,038 (12%) | เช็คอินช้า/เร็วกว่านัดกี่นาที — null = ไม่ได้เช็คอิน |
| `tx_record_offset_min` | int | 0 | เวลาบันทึกหัตถการ - เวลานัด (นาที) |
| `receipt_offset_min` | float | 360 (0.3%) | เวลาออกใบเสร็จ - เวลานัด (นาที) |

### Status flags

| Column | Type | Nulls | คำอธิบาย |
|--------|------|-------|----------|
| `checked_in` | int | 0 | เช็คอินแล้วหรือยัง (0/1) |
| `treatment_recorded` | int | 0 | บันทึกหัตถการแล้วหรือยัง (0/1) |
| `receipt_issued` | int | 0 | ออกใบเสร็จแล้วหรือยัง (0/1) |

---

## Target Variable

**แนะนำ:** `scheduled_duration_min`

```
count    120,534
mean      28.1 นาที
median    30.0 นาที
std       14.4 นาที
min        5 นาที
max      260 นาที
p25       15 นาที
p75       30 นาที
```

Distribution เบ้ไปทางขวา — หัตถการส่วนใหญ่ 15-30 นาที แต่มี case ยาว ๆ ถึง 260 นาที

---

## สถิติข้อมูล

| ตัวเลข | ค่า |
|--------|-----|
| จำนวน rows | 120,534 |
| จำนวน columns | 18 |
| คลินิก (unique) | 147 |
| ทันตแพทย์ (unique) | 154 |
| หัตถการ (unique) | 5,350 |
| ช่วงเวลา | ม.ค. 2025 → เม.ย. 2025 (4 เดือน) |

### Top 10 หัตถการ

| อันดับ | หัตถการ | จำนวน |
|--------|---------|-------|
| 1 | ปรับเครื่องมือจัดฟัน | 4,417 |
| 2 | ขูดหินปูน | 3,448 |
| 3 | At — ปรับเครื่องมือจัดฟัน | 2,696 |
| 4 | ถอนฟันแท้ | 1,969 |
| 5 | เปลี่ยนยาง/ปรับเครื่องมือจัดฟัน | 1,844 |
| 6 | ปรับเครื่องมือ | 1,840 |
| 7 | * รวมค่าบริการทางการแพทย์ | 1,790 |
| 8 | อุดฟันคอมโพสิท 1 ด้าน | 1,746 |
| 9 | ค่าปลอดเชื้อครื่องมือ | 1,606 |
| 10 | เปลี่ยนยางจัดฟัน | 1,547 |

---

## คำแนะนำสำหรับ Feature Engineering

### Train / Test Split

**แนะนำ:** time-based split ด้วย `appt_year_month`

```python
train = df[df["appt_year_month"] <= "2025-03"]   # ม.ค. - มี.ค.
test  = df[df["appt_year_month"] == "2025-04"]    # เม.ย.
```

**ทำไม:** ป้องกัน data leakage — ใน production เราทำนายอนาคตจากอดีตเสมอ ไม่ใช่ random split

### Categorical Encoding

| Column | จำนวน unique | แนะนำ |
|--------|-------------|-------|
| `clinic_pseudo_id` | 147 | Target encoding |
| `dentist_pseudo_id` | 154 | Target encoding |
| `treatment` | 5,350 | Target encoding (high cardinality — ห้ามใช้ one-hot) |

**ทำไม target encoding:** เพราะ cardinality สูงมาก โดยเฉพาะ treatment ที่มี 5,350 ค่า — one-hot จะสร้าง column มากเกินไป

### Columns ที่ต้องระวัง

| Column | ปัญหา | แนวทาง |
|--------|-------|--------|
| `tooth_no` | null 67% | พิจารณา drop หรือแปลงเป็น has_tooth_no flag |
| `surfaces` | null 94% | พิจารณา drop หรือแปลงเป็น has_surfaces flag |
| `treatment` | 899 nulls | Drop rows หรือ impute ด้วย "unknown" |
| `checkin_delay_min` | null 12% | null = ไม่ได้เช็คอิน — อาจใช้ร่วมกับ `checked_in` flag |

---

## สิ่งที่ต้องรู้ (Known Limitations)

1. **pseudo_id เปลี่ยนทุกรอบ** — HMAC key สร้างใหม่ทุกครั้งที่รัน anonymize → clinic C_abc ในรอบนี้ ≠ C_abc ในรอบหน้า → **ห้าม join ข้ามรอบการรัน**

2. **notes ไม่มีเนื้อหา** — มีแค่ `has_notes` flag (0/1) เพราะ notes อาจมี PII

3. **hour เป็น 4-hour bucket** — ไม่ใช่เวลาจริง เช่น 9:30 น. → bucket 8 — เพื่อป้องกัน re-identification

4. **~50% ของ raw data ถูก drop** — เพราะไม่มี license_no (dentist) → ไม่มี dentist_pseudo_id → ใช้ train model ไม่ได้

5. **k-anonymity (k=5)** — row ที่อยู่ในกลุ่ม (clinic + เดือน + วัน + ชั่วโมง) น้อยกว่า 5 คน ถูก drop เพื่อความเป็นส่วนตัว → อาจมี bias ต่อคลินิกเล็ก

6. **ข้อมูล 4 เดือน** — ม.ค. → เม.ย. 2025 — อาจไม่ครอบคลุม seasonal pattern ทั้งปี

---

## วิธีรัน pipeline เอง (ถ้าต้องการ data ใหม่)

```bash
# 1. Clone repo
gh repo clone natchyunicorn/denttime
cd denttime

# 2. ติดตั้ง dependencies
pip install "apache-airflow==3.2.0" pandas

# 3. ตั้ง Airflow
export AIRFLOW_HOME=~/Desktop/denttime_data/airflow_home
airflow db migrate

# 4. วาง CSV ใหม่ใน data/incoming/
cp new_export_*.csv data/incoming/

# 5. รัน pipeline
airflow dags test denttime_pipeline 2026-05-01

# 6. ผลอยู่ที่
ls data/published/2026-05-01/
#   data.csv        ← ใช้ตัวนี้
#   manifest.json   ← metadata
```

---

## เช็คลิสต์ตอนรับไฟล์

- [ ] `pd.read_csv()` สำเร็จ ไม่มี encoding error
- [ ] Shape = (120534, 18) ตรงกับ manifest.json
- [ ] ไม่มี column ที่เป็น PII (ชื่อคนไข้, เลขบัตร, ชื่อหมอ, ชื่อคลินิกจริง)
- [ ] `scheduled_duration_min` ไม่มี null, อยู่ในช่วง 5-260
- [ ] `treatment` null ไม่เกิน 1%
- [ ] `total_amount` ≥ 0 ทุก row
- [ ] Time-based split ใช้ `appt_year_month` ไม่ใช่ random
- [ ] ลอง train baseline model (เช่น mean predictor) เพื่อเทียบ baseline ก่อน

---

## ติดต่อ

มีคำถามเรื่อง data pipeline หรือ schema → ถาม Natcha
