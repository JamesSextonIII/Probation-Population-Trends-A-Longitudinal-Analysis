# Synthetic Data Generator created to simulate unclean probation records for ETL testing.
# Generates "dirty" data with intentional logical errors, type mismatches, and missing values.

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# --- CONFIGURATION ---
NUM_ROWS = 15000
OUTPUT_PATH = "04_data/raw/probation_dataset_dirty.csv"

# 22 Probation Agents (Mixed Gender)
OFFICERS = [
    "Sarah Connor", "James Rickards", "Elena Fisher", "Marcus Fenix", 
    "Lara Croft", "Nathan Drake", "Jill Valentine", "Chris Redfield",
    "Tifa Lockhart", "Barret Wallace", "Yuna Braska", "Tidus Jecht",
    "Garrus Vakarian", "Liara T'Soni", "Cortana John", "Master Chief",
    "Samus Aran", "Solid Snake", "Meryl Silverburgh", "Leon Kennedy",
    "Claire Redfield", "Ada Wong"
]

def generate_dirty_data():
    data = []
    
    for _ in range(NUM_ROWS):
        # 1. DOC Number (Unique ID)
        doc_num = random.randint(100000, 999999)
        
        # 2. COMPAS Score (The Dirt: Mixed types and inconsistencies)
        # 80% clean (1-10), 10% Text (High/Med/Low), 10% Missing
        rand_val = random.random()
        if rand_val < 0.8:
            compas = random.randint(1, 10)
        elif rand_val < 0.9:
            compas = random.choice(["High", "Medium", "Low", "Hi", "Med", "Lo"])
        else:
            compas = np.nan # Missing value

        # 3. Dates (The Dirt: Logical errors)
        # Random start date in the last 5 years
        start_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
        sentence_date = start_date.strftime("%Y-%m-%d")
        
        # Discharge is usually 1-3 years later
        proj_end_date = start_date + timedelta(days=random.randint(365, 1095))
        discharge_date = proj_end_date.strftime("%Y-%m-%d")
        
        # Actual Discharge (The Dirt: Data entry errors)
        outcome = random.choice(["Successful", "Revoked", "Absconded", "Deceased", "Active"])
        
        if outcome == "Active":
            actual_discharge = np.nan
        else:
            # 5% chance of data entry error where Actual < Sentence (Impossible date)
            if random.random() < 0.05:
                bad_date = start_date - timedelta(days=random.randint(10, 100))
                actual_discharge = bad_date.strftime("%Y-%m-%d")
            else:
                # Normal variance
                actual_discharge = (proj_end_date + timedelta(days=random.randint(-60, 60))).strftime("%Y-%m-%d")

        # 4. Discharge Type (The Dirt: Inconsistent capitalization/spelling)
        if outcome == "Revoked":
            outcome = random.choice(["Revoked", "revoked", "Revoked - Tech", "Revoked-New Crime"])
        
        # 5. Assigned Officer
        officer = random.choice(OFFICERS)

        data.append([doc_num, compas, sentence_date, discharge_date, actual_discharge, outcome, officer])

    # Create DataFrame
    columns = [
        "DOC_Number", "COMPAS_Score", "Sentence_Date", 
        "Discharge_Date", "Actual_Discharge_Date", 
        "Discharge_Type", "Assigned_Officer"
    ]
    df = pd.DataFrame(data, columns=columns)
    
    # Introduce duplicate rows (clerical errors)
    df = pd.concat([df, df.sample(n=50)], ignore_index=True)
    
    return df

if __name__ == "__main__":
    print("Generating synthetic probation data...")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Generate and Save
    df = generate_dirty_data()
    df.to_csv(OUTPUT_PATH, index=False)
    
    print(f"Success! Generated {len(df)} rows at {OUTPUT_PATH}")