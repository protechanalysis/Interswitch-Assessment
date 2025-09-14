WITH EmployeeHierarchy AS (
 
    SELECT
        e.employee_id,
        e.first_name + ' ' + e.last_name AS employee_name,
        e.manager_id,
        CAST(NULL AS NVARCHAR(MAX)) AS manager_name, 
        0 AS level
    FROM Employee e

    UNION ALL


    SELECT
        e.employee_id,
        e.first_name + ' ' + e.last_name AS employee_name,
        e.manager_id,
        CAST(eh.employee_name AS NVARCHAR(MAX)) AS manager_name,  
        eh.level + 1
    FROM Employee e
    INNER JOIN EmployeeHierarchy eh
        ON e.manager_id = eh.employee_id
)
SELECT
    employee_name AS Employee,
    manager_name AS Reports_To,
    level AS Hierarchy_Level
FROM EmployeeHierarchy
ORDER BY employee_name, level;
