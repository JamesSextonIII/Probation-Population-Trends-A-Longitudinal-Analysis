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

## 5. Pipeline Execution & SQL Integration
To bridge the gap between raw data and analysis, I implemented a strict ETL (Extract, Transform, Load) process in `01_data_cleaning/clean_probation_data.py`.

* **Logic Handling:** I utilized Pandas to programmatically detect and coerce specific errors, such as mapping the mixed-type `COMPAS_Score` to a unified integer scale and enforcing temporal consistency on Date columns.
* **Storage Strategy:** Instead of relying solely on CSV files, I integrated a **SQLite export step**. The cleaning script now loads the processed data directly into a local database (`probation_data.db`). This decision allows the subsequent analysis phase to utilize SQL queries, mirroring a professional enterprise environment.

## 6. Phase 2: Advanced SQL Analysis & Deduplication
With the database established, I shifted to SQL for high-precision auditing and analysis.

* **Data Quality Audit:** Simple Python deduplication removed exact matches, but deep SQL inspection revealed "fuzzy" duplicates (triplets) where offender IDs matched but data entry dates differed.
* **Golden Record Architecture:** Instead of destructively deleting rows, I engineered a SQL View (`view_probation_unique`) using Window Functions (`ROW_NUMBER() OVER PARTITION BY`). This programmatic approach guarantees that only the most recent administrative record is used for reporting, without losing the ability to audit historical conflicts.
* **Business Logic:** I authored complex aggregation queries (`03_recidivism_by_score.sql`) using `CASE` statements to calculate recidivism rates, avoiding common SQL pitfalls like integer division by utilizing conditional floating-point logic.

## 7. Final Data Validation & Trend Correction
While Phase 2 validated the SQL logic, the initial synthetic data unexpectedly showed a **flat recidivism trend**, undermining the visualization objective.

* **Correction:** I engineered a secondary update to `generate_data.py`, implementing a **probability weighting algorithm** (Base 5% Risk + Score * 8.5%) to programmatically force a clear correlation between the Risk Score and Recidivism.
* **Validation:** Final SQL query validation confirmed the fix, showing the recidivism rate now rising sharply from **~15% (Score 1)** to **~90% (Score 10)**. This confirms the business logic is sound and the data is fully ready for presentation.

## 8. Phase 3: Visualization & Reporting
**Status:** Complete
**Tooling:** Power BI Desktop via ODBC Driver (SQLite Connection)

To finalize the pipeline, I built an interactive dashboard to present the analytical findings to non-technical stakeholders.

* **Enterprise Connectivity:** Since Power BI lacks native SQLite support, I configured a system-level **ODBC Data Source (DSN)** to bridge the gap, simulating a real-world connection to an on-premise SQL server.
* **DAX Implementation:** I wrote custom DAX measures (using `CALCULATE` and `DIVIDE`) to compute Recidivism Rates dynamically at the row level, ensuring accurate aggregation across different cuts of the data (e.g., by Officer vs. by Risk Score).
* **Visual Data Story:** The dashboard confirms the "God Mode" trend engineered in Phase 2.
    * **Trend Line:** Validates the strong positive correlation between Risk Score (1-10) and Recidivism Rate.
    * **100% Stacked Bar:** Visually demonstrates how the volume of "Revoked" outcomes expands as risk profiles increase.

### Final Dashboard Artifact
![Power BI Dashboard](03_visualization/dashboard_screenshot.png)

## 9. Project Conclusion
This portfolio project successfully demonstrates a full-stack data capability:
1.  **Engineered** a synthetic dataset with complex "dirty" attributes to mimic real-world administrative logs.
2.  **Architected** a secure, modular ETL pipeline in Python to clean and standardize mixed-type data.
3.  **Audited** records using advanced SQL Window Functions to resolve fuzzy duplicates.
4.  **Delivered** actionable business intelligence via a professional Power BI dashboard.