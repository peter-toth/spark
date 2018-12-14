CREATE TEMPORARY VIEW department AS SELECT * FROM VALUES
  (0, null, "ROOT"),
  (1, 0, "A"),
  (2, 1, "B"),
  (3, 2, "C"),
  (4, 2, "D"),
  (5, 0, "E"),
  (6, 4, "F"),
  (7, 5, "G")
  AS department(id, parent_department_id, name);

WITH subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT * FROM subdepartment
)
SELECT id, parent_department_id, name FROM subdepartment;

WITH subdepartment AS (
  SELECT * FROM subdepartment
  UNION ALL
  SELECT * FROM department WHERE name = 'A'
)
SELECT id, parent_department_id, name FROM subdepartment;

WITH MAXIMUM 1 LEVEL RECURSION subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT * FROM subdepartment
)
SELECT id, parent_department_id, name FROM subdepartment;

WITH subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT d.*
  FROM department AS d
  JOIN subdepartment AS sd ON (sd.id = d.parent_department_id)
)
SELECT id, parent_department_id, name FROM subdepartment ORDER BY id;

WITH subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT d.*
  FROM department AS d
  JOIN subdepartment AS sd ON (sd.parent_department_id = d.id)
)
SELECT id, parent_department_id, name FROM subdepartment ORDER BY id;


WITH subdepartment AS (
  SELECT DISTINCT * FROM (
    SELECT * FROM department WHERE name = 'A'
    UNION ALL
    SELECT * FROM subdepartment
  )
)
SELECT id, parent_department_id, name FROM subdepartment ORDER BY id;
