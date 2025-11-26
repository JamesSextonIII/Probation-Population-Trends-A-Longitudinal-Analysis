-- Create a "Golden Record" View for Analysis
-- This view filters out the 113+ duplicates identified in the audit.
-- Use ROW_NUMBER() to keep ONLY the most recent record per Offender.

CREATE VIEW view_probation_unique AS

WITH Ranked_Records AS (
    SELECT 
        *,
        -- Window Function: Assigns a rank (1, 2, 3...) to each record within the same DOC_Number group.
        -- The record with the LATEST Actual_Discharge_Date gets Row_Num = 1.
        ROW_NUMBER() OVER (
            PARTITION BY DOC_Number 
            ORDER BY Actual_Discharge_Date DESC
        ) as Row_Num
    FROM 
        probation_records
)
-- Final Selection: Keep only the "Survivor" (Rank 1)
SELECT 
    DOC_Number,
    COMPAS_Score_Clean,
    Sentence_Date,
    Discharge_Date,
    Actual_Discharge_Date,
    Discharge_Type,
    Assigned_Officer
FROM 
    Ranked_Records
WHERE 
    Row_Num = 1;