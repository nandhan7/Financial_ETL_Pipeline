# 🧠 SP500 Financial ETL Pipeline with PySpark & Snowflake

This project demonstrates a real-time ETL pipeline built using **PySpark**, with transformation logic, logging, unit testing,scripting and scheduling. The pipeline extracts S&P 500 stock price data, performs cleaning and transformations, and loads the processed data into a destination directory and Snowflake.

---

## 📁 Project Structure

```
├── data/ # Output data files
│ └── data.csv
├── etl/ # ETL logic
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ ├── logger.py
│ └── main.py
├── logs/
│ └── etl_pipeline.log # Logging output
├── scripts/
│ └── run_etl.sh # Shell script to trigger ETL
├── tests/ # Unit tests
│ ├── test_load.py
│ └── test_transform.py
├── etl_scheduler.py # Python scheduler
└── README.md
```


---

## ⚙️ Technologies Used

- **Python** 3.10.9  
- **PySpark** 3.5.6  
- **Java** OpenJDK 1.8.0_462 (Temurin)  
- **Snowflake** (for loading final data)
- **Schedule** (Python library for periodic job execution)
- **Pytest** (for testing)
- **Shell scripting**

---

## 🏗️ ETL Workflow

1. **Extract**:
   - Load the S&P 500 dataset from Kaggle (CSV format).

2. **Transform** (using PySpark):
   - Drop rows with nulls in `open`, `close`, `volume`, `date`, `Name`
   - Convert `date` to proper DateType
   - Add `daily_change = close - open`
   - Filter where `volume > 1,000,000`
   - Add 7-day rolling average of close price for each stock
   - Rename `date` → `stock_date`

3. **Load**:
   - Save transformed data to `/data/data.csv`
   - Also load data to Snowflake table

4. **Logging**:
   - Logs each step to `logs/etl_pipeline.log`

5. **Scheduler**:
   - `etl_scheduler.py` runs the pipeline periodically every few minutes using the `schedule` library.

---

## 🧪 Running Tests

Tests are written using `pytest`.

```bash
pytest tests/
```


## 🔧 Setup Instructions

### ✅ Prerequisites

- Python `3.10.9`
- PySpark `3.5.6`
- Java `1.8+`
- Snowflake account (for data loading)

---

### 📦 Installation

```bash
# Create virtual environment
python -m venv .venv

# Activate the virtual environment
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

```

🔄 Running the ETL Pipeline
 Option 1: Manual Run Using Shell Script
```
.run_etl.sh
```
Option 2: Run via Scheduler
```
python etl_scheduler.py
```

This will schedule your ETL job to run every minute (as per your schedule.every(1).minutes.do() setup).

---

## 🧼 Transformations Applied

From `etl/transform.py`:

- Drop rows with nulls in critical columns: `open`, `close`, `volume`, `date`, `Name`
- Convert `date` to `DateType`
- Add new column `daily_change = close - open`
- Filter where `volume > 1,000,000`
- Add `seven_day_avg_close` per stock using rolling average window
- Rename `date` to `stock_date`

---

## ❄️ Snowflake Integration

Before running `load.py`, set the following environment variables or provide them via config:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_TABLE`

The load script appends data to the specified Snowflake table.

---
## 🛠 Java Version

Ensure Java 1.8 is installed:

```bash
java -version
```

Expected Output:

```
openjdk version "1.8.0_462"
OpenJDK Runtime Environment (Temurin)(build 1.8.0_462-b08)
OpenJDK 64-Bit Server VM (Temurin)(build 25.462-b08, mixed mode)
```
---

## 🧰 Technologies Used
- Python 3.10.9  
- PySpark 3.5.6  
- Shell Scripting  
- Snowflake Connector for Python  
- Python Logging  
- schedule (for ETL job scheduling)  
- Pytest (for unit testing)  
- Data Warehousing (Snowflake)  

## 📓 License
This project is licensed under the MIT License. Feel free to fork and adapt!



