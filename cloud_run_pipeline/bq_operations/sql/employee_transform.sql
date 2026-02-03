-- INSERT INTO `dataflow-pipeline-485105.organization.employees_advanced`
-- SELECT
--     *
-- FROM `dataflow-pipeline-485105.organization.employees_advanced`
--     QUALIFY ROW_NUMBER() OVER(
--     PARTITION BY emp_id
--     ORDER BY join_date DESC
--     ) = @id_to_find



SELECT
    first_name,
    last_name,
    salary,
    dept.dept_name,
    emp.join_date,
    ROW_NUMBER() OVER(PARTITION BY emp.dept_id ORDER BY emp.join_date DESC) AS latest_hire_dept
FROM `dataflow-pipeline-485105.organization.employees` emp
LEFT JOIN `dataflow-pipeline-485105.organization.department` dept
ON emp.dept_id=dept.dept_id
QUALIFY latest_hire_dept=1;
