-- Calculate Recidivism Rate by Risk Score
-- Uses 'view_probation_unique' to ensure we don't double-count duplicates.
-- Recidivism defined as Discharge_Type 'revoked' or 'absconded'.

SELECT 
    COMPAS_Score_Clean as Risk_Score,
    
    -- 1. Count Total Offenders per Score
    COUNT(*) as Total_Cases,
    
    -- 2. Count Failures (Revoked/Absconded) using CASE Logic
    SUM(CASE 
        WHEN Discharge_Type IN ('revoked', 'absconded') THEN 1 
        ELSE 0 
    END) as Recidivism_Count,
    
    -- 3. Calculate Percentage (Multiplied by 1.0 to force float division)
    ROUND(
        (SUM(CASE WHEN Discharge_Type IN ('revoked', 'absconded') THEN 1 ELSE 0 END) * 1.0 
        / COUNT(*)) * 100, 
    2) as Recidivism_Rate_Percent

FROM 
    view_probation_unique
WHERE 
    COMPAS_Score_Clean IS NOT NULL
GROUP BY 
    COMPAS_Score_Clean
ORDER BY 
    COMPAS_Score_Clean ASC;