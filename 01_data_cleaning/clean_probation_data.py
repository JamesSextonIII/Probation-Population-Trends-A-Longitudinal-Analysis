import pandas as pd
import numpy as np
import sqlite3
import os

# --- CONFIGURATION ---
INPUT_PATH = "04_data/raw/probation_dataset_dirty.csv"
OUTPUT_CSV = "04_data/processed/probation_cleaned.csv"
OUTPUT_DB  = "04_data/processed/probation_data.db"

def clean_data():
    print("--- Starting Data Cleaning Process ---")
    
    # 1. Load Validation
    # Ensure input file exists to prevent silent failures later in the pipeline.
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file not found at {INPUT_PATH}.")
    
    df = pd.read_csv(INPUT_PATH)

    # 2. Column Standardization
    # Normalize headers: Lowercase and replace spaces with underscores to ensure compatibility with SQL/Python syntax.
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # 3. Deduplication
    # Duplicate records risk inflating caseload statistics. We drop exact matches.
    df = df.drop_duplicates()

    # 4. Complex Transformation: COMPAS Scores
    # Goal: Normalize a mixed-type column (Strings/Integers) into a clean Integer scale (1-10).
    score_map = {
        'Low': 2, 'Lo': 2, 
        'Medium': 5, 'Med': 5, 
        'High': 8, 'Hi': 8, 'H': 8
    }
    
    # Step A: Extract existing integers. 'coerce' forces text strings ("High") to become NaN.
    numeric_scores = pd.to_numeric(df['compas_score'], errors='coerce')
    
    # Step B: Extract text strings. Map keys ("High") to their numeric equivalent (8).
    text_scores = df['compas_score'].map(score_map)
    
    # Step C: Combine. Fill gaps in the numeric series using values from the text map.
    df['compas_score_clean'] = numeric_scores.fillna(text_scores)
    
    # Step D: Impute. Fill remaining NaNs with Median to preserve dataset size without skewing distribution.
    median_score = df['compas_score_clean'].median()
    df['compas_score_clean'] = df['compas_score_clean'].fillna(median_score).astype(int)

    # 5. Date Standardization
    # Convert all temporal columns to datetime objects to enable time-delta calculations.
    date_cols = ['sentence_date', 'discharge_date', 'actual_discharge_date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 6. Logic Checking
    # Identify logically impossible records where the Discharge Date precedes the Start Date.
    mask_impossible = df['actual_discharge_date'] < df['sentence_date']
    
    # Set invalid dates to NaT (Not a Time) rather than dropping the row. 
    # This preserves the rest of the offender's data (Demographics, Risk) for other analyses.
    df.loc[mask_impossible, 'actual_discharge_date'] = pd.NaT

    # 7. Categorical Cleanup
    # Standardize string variations (e.g., "Revoked - Tech" -> "revoked") for accurate aggregation.
    df['discharge_type'] = df['discharge_type'].astype(str).str.lower().str.strip()
    
    type_map = {
        'revoked - tech': 'revoked',
        'revoked-new crime': 'revoked',
        'revoked': 'revoked',
        'successful': 'successful',
        'absconded': 'absconded',
        'deceased': 'deceased',
        'active': 'active'
    }
    df['discharge_type'] = df['discharge_type'].map(type_map).fillna('other')

    # 8. Export Stages
    # Save a CSV backup for manual inspection.
    df.to_csv(OUTPUT_CSV, index=False)

    # Export to SQLite for the Analysis phase.
    # 'if_exists="replace"' ensures we overwrite old tables completely on re-runs to avoid duplicate data.
    conn = sqlite3.connect(OUTPUT_DB)
    df.to_sql("probation_records", conn, if_exists="replace", index=False)
    conn.close()
    
    print(f"Success. Database created at {OUTPUT_DB}")

if __name__ == "__main__":
    clean_data()