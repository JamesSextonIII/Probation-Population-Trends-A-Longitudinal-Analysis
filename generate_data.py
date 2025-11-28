# Synthetic Data Generator created to simulate unclean probation records for ETL testing.
# Generates "dirty" data with intentional logical errors, type mismatches, and missing values.
# UPDATE: "God Mode" weighting applied to force strong correlation between Risk Score and Recidivism.

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
        
        # 4. EXTREMELY Weighted Outcome Logic (The Portfolio Story)
        # We want Recidivism to correlate strongly with COMPAS Score.
        
        # Risk Factor Calculation (Robust check for dirty data)
        try:
            risk_factor = int(compas)
        except:
            # If it's text (High/Low) or NaN, assign a rough integer for the trend calculation
            s_compas = str(compas).lower()
            if "h" in s_compas: risk_factor = 8
            elif "m" in s_compas: risk_factor = 5
            elif "l" in s_compas: risk_factor = 2
            else: risk_factor = 5 # Default to medium risk if dirty/missing

        # Formula: Base 5% risk + (Score * 8.5%). 
        # Score 1 = ~13% Risk. Score 10 = ~90% Risk.
        failure_prob = 0.05 + (risk_factor * 0.085)
        
        # Roll the dice
        if random.random() < failure_prob:
            # If failure, choose from recidivism types
            outcome = random.choice(["Revoked", "Revoked", "Revoked - Tech", "Revoked-New Crime", "Absconded"])
        else:
            # If successful, choose from non-recidivism types
            outcome = random.choice(["Successful", "Successful", "Successful", "Deceased", "Active"])

        # Set Actual Discharge Date based on outcome
        if outcome == "Active":
            actual_discharge = np.nan
        else:
            # If they failed, they likely failed EARLY (before projected end)
            if "Revoked" in outcome or "Absconded" in outcome:
                 actual_discharge = (start_date + timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d")
            else:
                 # If successful, they finished near the end date
                 actual_discharge = (proj_end_date + timedelta(days=random.randint(-30, 30))).strftime("%Y-%m-%d")
        
        # Inject the "Impossible Date" error (5% chance)
        if actual_discharge is not np.nan and random.random() < 0.05:
             bad_date = start_date - timedelta(days=random.randint(10, 100))
             actual_discharge = bad_date.strftime("%Y-%m-%d")

        # 5. Discharge Type Typos (The Dirt: Inconsistent capitalization/spelling)
        if "Revoked" in outcome and "Tech" not in outcome and "New" not in outcome:
             outcome = random.choice(["Revoked", "revoked", "Revoked"])
        
        # 6. Assigned Officer
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