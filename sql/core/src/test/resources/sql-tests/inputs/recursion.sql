-- List of configuration the test suite is run against:
--SET spark.sql.autoBroadcastJoinThreshold=10485760
--SET spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.adaptive.enabled=true

-- fails due to recursion isn't allowed
WITH r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r;

-- very basic recursion
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r ORDER BY level;

-- unlimited recursion fails at spark.sql.recursion.level.limit level
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r
)
SELECT * FROM r ORDER BY level;

-- recursion works regardless the order of anchor and recursive terms
WITH RECURSIVE r AS (
  SELECT level + 1, data FROM r WHERE level < 10
  UNION ALL
  VALUES (0, 'A') AS T(level, data)
)
SELECT * FROM r ORDER BY level;

-- multiple anchor terms are supported
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 3
  UNION ALL
  VALUES (0, 'B') AS T(level, data)
)
SELECT * FROM r ORDER BY level;

-- multiple recursive terms are supported
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 3
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 3
)
SELECT * FROM r ORDER BY level;

-- multiple anchor and recursive terms are supported
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  VALUES (0, 'B') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 3
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 3
)
SELECT * FROM r ORDER BY level;

-- recursive query should contain UNION ALL statements only
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  INTERSECT
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r;

-- recursion without any anchor term fails
WITH RECURSIVE r AS (
  SELECT level + 1, data FROM r WHERE level < 3
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 3
)
SELECT * FROM r;

-- recursive reference is not allowed in a subquery
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE (SELECT SUM(level) FROM r) < 10
)
SELECT * FROM r;

-- recursive reference is not allowed on both side of an inner join (self join)
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, r1.data
  FROM r AS r1
  JOIN r AS r2 ON r2.data = r1.data
)
SELECT * FROM r;

-- recursive reference is not allowed on right side of a left outer join
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, r.data
  FROM (SELECT 'B' AS data) AS o
  LEFT JOIN r ON r.data = o.data
)
SELECT * FROM r;

-- recursive reference is not allowed on left side of a right outer join
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  RIGHT JOIN (SELECT 'B' AS data) AS o ON o.data = r.data
)
SELECT * FROM r;

-- aggregate is supported in the anchor term
WITH RECURSIVE r AS (
  SELECT MAX(level) AS level, SUM(data) AS data FROM VALUES (0, 1), (0, 2) AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r ORDER BY level;

-- recursive reference is not allowed in an aggregate in a recursive term
WITH RECURSIVE r AS (
  VALUES (0, 1L) AS T(group, data)
  UNION ALL
  SELECT 1, SUM(data) FROM r WHERE data < 10 GROUP BY group
)
SELECT * FROM r;

-- recursive reference is not allowed in an aggregate (made from project) in a recursive term
WITH RECURSIVE r AS (
  VALUES (1L) AS T(data)
  UNION ALL
  SELECT SUM(data) FROM r WHERE data < 10
)
SELECT * FROM r;

-- aggregate is supported on a recursive table
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT level + 1, data FROM r WHERE level < 10
)
SELECT COUNT(*) FROM r;

-- recursive reference is not allowed to use in combination with distinct
WITH RECURSIVE r AS (
  VALUES (0, 'A') AS T(level, data)
  UNION ALL
  SELECT DISTINCT level + 1, data FROM r WHERE level < 10
)
SELECT * FROM r;

-- department structure represented here is as follows:
--
-- ROOT-->A-->B-->C
--   |    |
--   |    ∨
--   |    D-->F
--   ∨
--   E-->G
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

-- departments under 'A'
WITH RECURSIVE subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT d.*
  FROM department AS d
  JOIN subdepartment AS sd ON (sd.id = d.parent_department_id)
)
SELECT id, name FROM subdepartment ORDER BY name;

-- departments above 'A'
WITH RECURSIVE subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT d.*
  FROM department AS d
  JOIN subdepartment AS sd ON (sd.parent_department_id = d.id)
)
SELECT id, name FROM subdepartment ORDER BY name;

-- routes represented here is as follows:
--
-- New York<->Boston
--   |          ∧
--   ∨          |
-- Washington---+
--   |
--   ∨
-- Raleigh
CREATE TEMPORARY VIEW routes AS SELECT * FROM VALUES
  ('New York', 'Washington'),
  ('New York', 'Boston'),
  ('Boston', 'New York'),
  ('Washington', 'Boston'),
  ('Washington', 'Raleigh')
  AS routes(origin, destination);

-- handling cycles that could cause infinite recursion
WITH RECURSIVE destinations_from_new_york AS (
  SELECT 'New York' AS destination, ARRAY('New York') AS path, 0 AS length
  UNION ALL
  SELECT r.destination, CONCAT(d.path, ARRAY(r.destination)), d.length + 1
  FROM routes AS r
  JOIN destinations_from_new_york AS d ON d.destination = r.origin AND NOT ARRAY_CONTAINS(d.path, r.destination)
)
SELECT * FROM destinations_from_new_york;

-- Fibonacci numbers
WITH RECURSIVE fibonacci AS (
  VALUES (0, 1) AS T(a, b)
  UNION ALL
  SELECT b, a + b FROM fibonacci WHERE a < 10
)
SELECT a FROM fibonacci ORDER BY a;

-- solving Sudoku
WITH RECURSIVE sudoku AS (
  VALUES (
    ARRAY(
      0, 0, 6, 0, 2, 3, 0, 0, 1,
      5, 0, 0, 0, 0, 0, 9, 0, 0,
      0, 0, 2, 0, 0, 4, 0, 0, 0,
      2, 0, 0, 8, 0, 0, 0, 9, 3,
      0, 0, 1, 0, 0, 7, 0, 0, 0,
      8, 3, 0, 0, 0, 0, 4, 0, 0,
      6, 0, 0, 1, 0, 0, 5, 0, 4,
      0, 5, 0, 0, 0, 0, 6, 0, 0,
      0, 0, 9, 0, 7, 0, 0, 2, 0
    ),
    0
  ) AS T(puzzle, level)
  UNION ALL
  SELECT
    CONCAT(SLICE(puzzle, 1, newPosition - 1), ARRAY(newValue), SLICE(puzzle, newPosition + 1, 9 * 9 - newPosition)),
    level + 1
  FROM (
    SELECT
      puzzle,
      newPosition,
      EXPLODE(allowedValues) AS newValue,
      level
    FROM (
      SELECT
        puzzle,
        newPosition,
        ARRAY_EXCEPT(
          ARRAY_EXCEPT(
            ARRAY_EXCEPT(
              SEQUENCE(1, 9),
              -- used values in row
              SLICE(puzzle, FLOOR((newPosition - 1) / 9) * 9 + 1, 9)
            ),
            -- used values in column
            TRANSFORM(puzzle, (x, i) -> IF(i % 9 = (newPosition - 1) % 9, x, 0))
          ),
          -- used values in 3x3 block
          TRANSFORM(
            SLICE(puzzle, FLOOR((newPosition - 1) / (3 * 9)) * 3 * 9 + 1, 3 * 9),
            (x, i) -> IF(FLOOR(i / 3) % 3 = FLOOR((newPosition - 1) / 3) % 3, x, 0)
          )
        ) AS allowedValues,
        level
      FROM sudoku
      JOIN (SELECT EXPLODE(SEQUENCE(1, 9 * 9)) AS newPosition) ON puzzle[newPosition - 1] = 0
      ORDER BY SIZE(allowedValues)
      LIMIT 1
    )
  )
)
SELECT * FROM sudoku WHERE NOT ARRAY_CONTAINS(puzzle, 0);
