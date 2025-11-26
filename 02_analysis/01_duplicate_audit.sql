-- Audit Non-Exact Duplicates 
-- The Python script removed exact row duplicates, but 'logical' duplicates remain 
-- where a DOC_Number exists multiple times with conflicting dates or officer assignments.

-- "Surviving Record" Analysis
-- We retrieve all columns for the duplicate IDs to determine which row is the 'master'.
-- Sorting by Discharge Date (DESC) places the most recent administrative entry at the top,
-- suggesting it is the most current and accurate record to keep.

SELECT 
    * FROM 
    probation_records 
WHERE 
    DOC_Number IN (
        -- Subquery: Isolate only the IDs that appear multiple times
        SELECT 
            DOC_Number
        FROM 
            probation_records
        GROUP BY 
            DOC_Number
        HAVING 
            COUNT(DOC_Number) > 1
    )
ORDER BY 
    DOC_Number ASC,             -- Group the specific Offender's records together
    Actual_Discharge_Date DESC; -- Prioritize the latest data entry (The Survivor)