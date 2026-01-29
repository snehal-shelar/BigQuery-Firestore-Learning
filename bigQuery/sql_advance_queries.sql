-- 1. Basic Filtering: Uses 'AND' and 'NOT EQUAL' (<>) to isolate specific rows.
SELECT * FROM `organization.department`
WHERE location='India' AND dept_name<>'DevOps';

-- 2. Pagination & Sorting: Orders by date and uses LIMIT/OFFSET (essential for API development).
SELECT * FROM `organization.employees`
WHERE join_date>'2021-03-15' ORDER BY join_date DESC LIMIT 2 OFFSET 2;

-- 3. Basic Join & Pattern Matching: Combines two tables and uses 'LIKE' for flexible string searching.
SELECT first_name, last_name, dept_name, location FROM `organization.department` AS dept
JOIN `organization.employees` AS emp
  ON dept.dept_id=emp.dept_id
WHERE location LIKE '%Ind%';

-- 4. Multi-Table Join: Connects three tables to create a comprehensive view of staff, projects, and departments.
SELECT
  emp_id, first_name, last_name, proj_name, dept_name
FROM `organization.employees` AS emp
JOIN `organization.project` AS proj
  ON emp.dept_id=proj.dept_id
JOIN `organization.department` AS dept
  ON proj.dept_id=dept.dept_id;

-- 5. Left Join: Ensures parent data (Department) is shown even if no child data (Project) exists.
SELECT dept_name, proj_name, proj_id FROM `organization.department` AS dept
LEFT JOIN `organization.project` AS proj
  ON dept.dept_id=proj.dept_id;

-- 6. Simple Aggregation: Counts occurrences of IDs grouped by a specific category.
SELECT dept_id, COUNT(emp_id) AS emp_cnt FROM `organization.employees` GROUP BY dept_id;

-- 7. Filtering Aggregates: Uses 'HAVING' to filter groups (logic that cannot be done with 'WHERE').
SELECT COUNT(proj_id) AS proj_cnt, dept_id FROM `organization.project`
GROUP BY dept_id HAVING proj_cnt>1;

-- 8. Sub-query: Uses a nested SELECT to filter the main query based on values from another table.
SELECT emp_id, first_name, last_name
FROM `organization.employees`
WHERE dept_id IN (
  SELECT dept_id FROM
  `organization.department`
  WHERE budget>500000
);

-- 9. Advanced Grouping: Performs multiple math calculations (SUM, AVG) across joined tables.
SELECT
  SUM(dept.budget) AS total_dept_budget,
  AVG(dept.budget) AS average_dept_budget,
  COUNT(emp.emp_id) AS emp_cnt,
  dept.location
FROM `organization.department` AS dept
JOIN `organization.employees` AS emp
  ON dept.dept_id=emp.dept_id
GROUP BY dept.location;

-- 10. The Final Boss: A complex query combining Left Joins, Distinct Counts, and Grouping for a high-level report.
SELECT
  dept.dept_name,
  dept.budget,
  COUNT(DISTINCT emp.emp_id) AS emp_cnt,
  COUNT(DISTINCT proj.proj_id) AS proj_cnt,
FROM `organization.department` AS dept
LEFT JOIN `organization.employees` AS emp
  ON dept.dept_id=emp.dept_id
LEFT JOIN `organization.project` AS proj
  ON dept.dept_id=proj.dept_id
WHERE dept.location='India'
GROUP BY dept.dept_name, dept.budget;

