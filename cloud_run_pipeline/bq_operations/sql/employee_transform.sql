-- INSERT INTO `dataflow-pipeline-485105.organization.employees_advanced`
-- SELECT
--     *
-- FROM `dataflow-pipeline-485105.organization.employees_advanced`
--     QUALIFY ROW_NUMBER() OVER(
--     PARTITION BY emp_id
--     ORDER BY join_date DESC
--     ) = @id_to_find


INSERT INTO `dataflow-pipeline-485105.organization.latest_hire_dept`
  (id, first_name, last_name, dept_name, join_date, dept_id, latest_hire_dept)
SELECT
  emp_id,
  first_name,
  last_name,
  dept.dept_name,
  emp.join_date,
  emp.dept_id,
  ROW_NUMBER()
    OVER (PARTITION BY emp.dept_id ORDER BY emp.join_date DESC)
    AS latest_hire_dept
FROM `dataflow-pipeline-485105.organization.employees` emp
LEFT JOIN `dataflow-pipeline-485105.organization.department` dept
  ON emp.dept_id = dept.dept_id
QUALIFY latest_hire_dept = 1;

