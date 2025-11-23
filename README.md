## Project Architecture & Reproducibility
To ensure the analysis is reproducible and modular, this project follows a strict directory structure:

* **`01_data_cleaning/`**: Scripts to ingest raw data and handle null values/inconsistencies.
* **`02_analysis/`**: Exploratory Data Analysis (EDA) and statistical modeling.
* **`03_visualization/`**: Dashboard assets and final reporting outputs.
* **`04_data/`**: Local storage for input/output files. Divided into `raw` (read-only) and `processed` (clean).

## Data Privacy & Ethics
**Note on Data Availability:**
Due to the sensitive nature of probation records (PII), the raw dataset (`data/raw`) is **not included** in this repository.
* A `.gitignore` file was configured to strictly exclude all Excel/CSV files in the data directory to prevent accidental leakage of sensitive information.
* To run this code, a user would need to supply their own synthetic or anonymized dataset matching the schema described in the data dictionary.

