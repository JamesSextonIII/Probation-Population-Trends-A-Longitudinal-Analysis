# Project Log: Probation Population Trends Analysis

## 1. Project Objective
I initiated this project to build an end-to-end data pipeline that mirrors the complexities of real-world criminal justice data. My primary goal was to demonstrate the ability to handle "dirty" administrative records, protect sensitive information (PII), and derive actionable insights regarding recidivism risks.

## 2. Architecture & Workflow Decisions
I deliberately moved away from a single-file approach to a modular architecture to ensure scalability and reproducibility.

* **Modular Separation:** I isolated the workflow into three distinct stages:
    * `01_data_cleaning/`: Raw ingestion and cleaning logic.
    * `02_analysis/`: Statistical exploration (EDA).
    * `03_visualization/`: Final reporting assets.
* **Data Isolation:** I created a dedicated `04_data/` directory to separate input/output files from code, ensuring that data processing steps are clear and traceable.

## 3. Data Privacy & Security Implementation
**Challenge:** Real probation data contains sensitive Personally Identifiable Information (PII) which cannot be hosted on public repositories.
**Solution:**
1.  **Git Ignore Strategy:** I configured `.gitignore` to strictly exclude all files within `04_data/raw/` and `04_data/processed/`. This ensures that even if I work with real files locally, they can never be accidentally pushed to GitHub.
2.  **Synthetic Data Generation:** To allow others to verify my code without compromising privacy, I wrote a Python script (`generate_data.py`) to manufacture a 15,000-row synthetic dataset that mathematically mimics the distribution and errors of real caseloads.

## 4. Engineering "Dirty" Data
To prove my data cleaning capabilities, I did not want a perfect dataset. I engineered specific data quality issues into the generation script to simulate a broken ETL pipeline:
* **Type Inconsistencies:** I forced the `COMPAS_Score` column to mix integers (1-10) with text strings ("High", "Med") to require robust type coercion.
* **Logical Fallacies:** I introduced date errors where `Actual_Discharge_Date` occurs *before* the `Sentence_Date`.
* **Clerical Errors:** I injected typos into categorical variables (e.g., "Revoked" vs "Revoked - Tech") and added duplicate rows to simulate data entry mistakes.

## 5. Next Steps
The immediate next phase of this project involves writing the cleaning scripts in `01_data_cleaning` to systematically detect, flag, and remediate the errors listed above.

## 5. Pipeline Execution & SQL Integration
To bridge the gap between raw data and analysis, I implemented a strict ETL (Extract, Transform, Load) process in `01_data_cleaning/clean_probation_data.py`.

* **Logic Handling:** I utilized Pandas to programmatically detect and coerce specific errors, such as mapping the mixed-type `COMPAS_Score` to a unified integer scale and enforcing temporal consistency on Date columns.
* **Storage Strategy:** Instead of relying solely on CSV files, I integrated a **SQLite export step**. The cleaning script now loads the processed data directly into a local database (`probation_data.db`). This decision allows the subsequent analysis phase to utilize SQL queries, mirroring a professional enterprise environment.

## 6. Next Steps: SQL Analysis
With the data cleaned and stored, the project moves to **Phase 2: Analysis**. I will utilize SQL to query the `probation_data.db` database, focusing on extracting insights regarding recidivism rates across different officer caseloads.