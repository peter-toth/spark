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

-- sum of 1..100
WITH RECURSIVE t AS (
  VALUES (1) AS T(n)
  UNION ALL
  SELECT n + 1 FROM t WHERE n < 100
)
SELECT SUM(n) FROM t;

-- terminate recursion without level
WITH RECURSIVE t AS (
  SELECT (VALUES(1)) AS n
  UNION ALL
  SELECT n + 1 FROM t WHERE n < 5
)
SELECT * FROM t;

-- using string column in recursion
WITH RECURSIVE t AS (
  SELECT 'foo' AS n
  UNION ALL
  SELECT n || ' bar' FROM t WHERE LENGTH(n) < 20
)
SELECT n FROM t;

---- TODO: Stack overflow, should work???
---- using inside subquery
-- SET spark.sql.cte.recursion.level.limit = 500;
--
--WITH RECURSIVE t1 AS (
--  SELECT 1 AS n
--  UNION ALL
--  SELECT n + 1 FROM t WHERE n < 500
--),
--t2 AS (
--  SELECT 1 AS n
--  UNION ALL
--  SELECT n + 1 FROM t WHERE n < 100
--)
--SELECT COUNT(*) FROM t1 WHERE n < (
--  SELECT COUNT(*) FROM (
--    SELECT * FROM t2 WHERE n < 50000
--  )
--  WHERE n < 100
--);

-- view based on recursion
CREATE TEMPORARY VIEW sums_1_100 AS
WITH RECURSIVE t AS (
  VALUES (1) AS T(n)
  UNION ALL
  SELECT n + 1 FROM t WHERE n < 100
)
SELECT SUM(n) FROM t;

SELECT * FROM sums_1_100;

-- recursive term has sub UNION
WITH RECURSIVE t AS (
  VALUES (1, 2) AS T(i, j)
  UNION ALL
  SELECT t2.i, t.j + 1
  FROM (
    SELECT 2 AS i
    UNION ALL SELECT 3 AS i
  ) AS t2
  JOIN t ON (t2.i = t.i + 1)
)
SELECT * FROM t;

-- unlimited recursion fails at spark.sql.cte.recursion.level.limits level
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
-- ROOT--->A--->B--->C
--  |      |
--  |      ∨
--  |      D--->F
--  ∨
--  E--->G
CREATE TEMPORARY VIEW department AS SELECT * FROM VALUES
  (0, null, 'ROOT'),
  (1, 0, 'A'),
  (2, 1, 'B'),
  (3, 2, 'C'),
  (4, 2, 'D'),
  (5, 0, 'E'),
  (6, 4, 'F'),
  (7, 5, 'G')
  AS department(id, parent_department, name);

-- all departments under 'A', result should be A, B, C, D and F
WITH RECURSIVE subdepartment AS (
	SELECT name as root_name, * FROM department WHERE name = 'A'
	UNION ALL
	SELECT sd.root_name, d.*
	FROM department AS d, subdepartment AS sd
	WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

-- all departments under 'A' with "level" number
WITH RECURSIVE subdepartment AS (
	SELECT 1 as level, id, parent_department, name FROM department WHERE name = 'A'
	UNION ALL
	SELECT sd.level + 1, d.*
	FROM department AS d, subdepartment AS sd
	WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

-- all departments under 'A' with "level" number, only shows level 2 or more
WITH RECURSIVE subdepartment AS (
	SELECT 1 as level, id, parent_department, name FROM department WHERE name = 'A'
	UNION ALL
	SELECT sd.level + 1, d.*
	FROM department AS d, subdepartment AS sd
	WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment WHERE level >= 2 ORDER BY name;

-- departments above 'A'
WITH RECURSIVE subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
  UNION ALL
  SELECT d.*
  FROM department AS d
  JOIN subdepartment AS sd ON (sd.parent_department = d.id)
)
SELECT id, name FROM subdepartment ORDER BY name;

-- RECURSIVE is ignored if the query has no self-reference
WITH RECURSIVE subdepartment AS (
  SELECT * FROM department WHERE name = 'A'
)
SELECT * FROM subdepartment ORDER BY name;

-- recursion via a VIEW
CREATE TEMPORARY VIEW vsubdepartment AS
  WITH RECURSIVE subdepartment AS (
    SELECT * FROM department WHERE name = 'A'
	UNION ALL
	SELECT d.*
	FROM department AS d, subdepartment AS sd
	WHERE d.parent_department = sd.id
  )
  SELECT * FROM subdepartment;

SELECT * FROM vsubdepartment ORDER BY name;

-- tree structure represented here is as follows:
--
-- ROOT--->1--->2---+--->4--->9--->14
--         |    |   |    |
--         |    ∨   ∨    ∨
--         |    5   6    10
--         ∨
--         3--->7---+--->11--->15
--         |    |   |    |
--         ∨    ∨   ∨    ∨
--         8    12  13   16
CREATE TEMPORARY VIEW tree AS SELECT * FROM VALUES
  (1, NULL),
  (2, 1),
  (3, 1),
  (4, 2),
  (5, 2),
  (6, 2),
  (7, 3),
  (8, 3),
  (9, 4),
  (10, 4),
  (11, 7),
  (12, 7),
  (13, 7),
  (14, 9),
  (15, 11),
  (16, 11)
AS (id, parent_id);

-- get all paths from "second level" nodes to leaf nodes
WITH RECURSIVE t AS (
  VALUES(1, ARRAY_REMOVE(ARRAY(0), 0)) AS T(id, path)
  UNION ALL
  SELECT tree.id, t.path || ARRAY(tree.id)
  FROM tree
  JOIN t ON tree.parent_id = t.id
)
SELECT t1.*, t2.*
FROM t AS t1
JOIN t AS t2 ON t1.path[0] = t2.path[0] AND SIZE(t1.path) = 1 AND SIZE(t2.path) > 1
ORDER BY t1.id, t2.id;

-- count all paths from "second level" nodes to leaf nodes
WITH RECURSIVE t AS (
  VALUES(1, ARRAY_REMOVE(ARRAY(0), 0)) AS T(id, path)
  UNION ALL
  SELECT tree.id, t.path || ARRAY(tree.id)
  FROM tree
  JOIN t ON tree.parent_id = t.id
)
SELECT t1.id, COUNT(t2.*)
FROM t AS t1
JOIN t AS t2 ON t1.path[0] = t2.path[0] AND SIZE(t1.path) = 1 AND SIZE(t2.path) > 1
GROUP BY t1.id
ORDER BY t1.id;

-- get all paths
WITH RECURSIVE t AS (
  VALUES(1, ARRAY_REMOVE(ARRAY(0), 0)) AS T(id, path)
  UNION ALL
  SELECT tree.id, t.path || ARRAY(tree.id)
  FROM tree
  JOIN t ON tree.parent_id = t.id
)
SELECT t1.id, t2.path, STRUCT(t2.*)
FROM t AS t1
JOIN t AS t2 ON t1.id = t2.id;

-- graph structure represented here is as follows:
--
-- +--->3
-- |    ∧
-- |    |
-- 2<---1--->4
--      ∧    |
--      |    ∨
--      +----5
CREATE TEMPORARY VIEW graph AS SELECT * FROM VALUES
  (1, 2, 'arc 1 -> 2'),
  (1, 3, 'arc 1 -> 3'),
  (2, 3, 'arc 2 -> 3'),
  (1, 4, 'arc 1 -> 4'),
  (4, 5, 'arc 4 -> 5'),
  (5, 1, 'arc 5 -> 1')
AS (f, t, label);

-- test cycle detection
WITH RECURSIVE search_graph AS (
  SELECT *, ARRAY(STRUCT(g.f, g.t)) AS path, false AS cycle FROM graph g
  UNION ALL
  SELECT g.*, path || ARRAY(STRUCT(g.f, g.t)), ARRAY_CONTAINS(path, STRUCT(g.f, g.t))
  FROM graph g, search_graph sg
  WHERE g.f = sg.t AND NOT cycle
)
SELECT * FROM search_graph;

-- ordering by the path column has same effect as SEARCH DEPTH FIRST
WITH RECURSIVE search_graph AS (
  SELECT *, ARRAY(STRUCT(g.f, g.t)) AS path, false AS cycle FROM graph g
  UNION ALL
  SELECT g.*, path || ARRAY(STRUCT(g.f, g.t)), ARRAY_CONTAINS(path, STRUCT(g.f, g.t))
  FROM graph g, search_graph sg
  WHERE g.f = sg.t AND NOT cycle
)
SELECT * FROM search_graph ORDER BY path;

-- test multiple WITH queries
WITH RECURSIVE y AS (
  VALUES (1) AS T(id)
),
x AS (
  SELECT * FROM y
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 5
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  VALUES (1) AS T(id)
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 5
),
y AS (
  VALUES (1) AS T(id)
  UNION ALL
  SELECT id + 1 FROM y WHERE id < 10
)
SELECT * FROM y LEFT JOIN x ON x.id = y.id;

WITH RECURSIVE x AS (
  VALUES (1) AS T(id)
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 5
),
y AS (
  VALUES (1) AS T(id)
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 10
)
SELECT * FROM y LEFT JOIN x ON x.id = y.id;

WITH RECURSIVE x AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 3
),
y AS (
  SELECT * FROM x
  UNION ALL
  SELECT * FROM x
),
z AS (
  SELECT * FROM x
  UNION ALL
  SELECT id + 1 FROM z WHERE id < 10
)
SELECT * FROM z;

WITH RECURSIVE x AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1 FROM x WHERE id < 3
),
y AS (
  SELECT * FROM x
  UNION ALL
  SELECT * FROM x
),
z AS (
  SELECT * FROM y
  UNION ALL
  SELECT id + 1 FROM z WHERE id < 10
)
SELECT * FROM z;

-- routes represented here is as follows:
--
-- New York<-->Boston
-- |           ∧
-- ∨           |
-- Washington--+
-- |
-- ∨
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

-- error cases
WITH RECURSIVE x AS (
  SELECT 1 AS n
  INTERSECT
  SELECT n + 1 FROM x
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  INTERSECT ALL
  SELECT n + 1 FROM x
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  EXCEPT
  SELECT n + 1 FROM x
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  EXCEPT ALL
  SELECT n + 1 FROM x
)
SELECT * FROM x;

WITH RECURSIVE x AS (
 SELECT n FROM x
)
SELECT * FROM x;

WITH RECURSIVE t AS (
  INSERT INTO y SELECT * FROM t
)
VALUES(FALSE);

CREATE TEMPORARY VIEW y AS SELECT EXPLODE(SEQUENCE(1, 10)) AS a;

WITH RECURSIVE x AS (
  SELECT a AS n FROM y WHERE a = 1
  UNION ALL
  SELECT x.n + 1 FROM y LEFT JOIN x ON x.n = y.a WHERE n < 10
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT a AS n FROM y WHERE a = 1
  UNION ALL
  SELECT x.n + 1 FROM x RIGHT JOIN y ON x.n = y.a WHERE n < 10
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT a AS n FROM y WHERE a = 1
  UNION ALL
  SELECT x.n + 1 FROM x FULL JOIN y ON x.n = y.a WHERE n < 10
) SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1
  FROM x
  WHERE n IN (SELECT * FROM x)
)
SELECT * FROM x;

-- aggregate functions
WITH RECURSIVE x AS (
  SELECT 1 AS n
  UNION ALL
  SELECT COUNT(*) FROM x
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  UNION ALL
  SELECT SUM(n) FROM x
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM x
  ORDER BY 1
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM x
  LIMIT 10
)
SELECT * FROM x;

WITH RECURSIVE x AS (
  VALUES (1) AS T(id)
  UNION ALL
  SELECT (
    SELECT * FROM x
  )
  FROM x
  WHERE id < 5
)
SELECT * FROM x;

-- mutual recursive query is not implemented,
-- y gets resolved as a view defined before and is not treated as forward reference
WITH RECURSIVE x AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1 FROM y WHERE id < 5
),
y AS (
  SELECT 1 AS id
  UNION ALL
  SELECT id + 1
  FROM x
  WHERE id < 5
)
SELECT * FROM x;

-- this kind of non-linear recursion is allowed
-- PostgreSQL doesn't allow it but MSSQL does
WITH RECURSIVE foo AS (
  VALUES (1) AS T(i)
  UNION ALL
  SELECT i + 1 FROM foo WHERE i < 10
  UNION ALL
  SELECT i + 1 FROM foo WHERE i < 5
)
SELECT i, COUNT(*) FROM foo GROUP BY i ORDER BY i;

-- this kind of non-linear recursion is not allowed, a recursive term can contain only one recursive reference
WITH RECURSIVE foo AS (
  VALUES (1) AS T(i)
  UNION ALL
  SELECT * FROM (
    SELECT i + 1
    FROM foo WHERE i < 10
    UNION ALL
    SELECT i + 1
    FROM foo WHERE i < 5
  ) AS t
)
SELECT i, COUNT(*) FROM foo GROUP BY i ORDER BY i;

-- this kind of non-linear recursion is not allowed, a recursive term can contain only one recursive reference
WITH RECURSIVE foo AS (
  VALUES (1) AS T(i)
  UNION ALL (
    SELECT i + 1 FROM foo WHERE i < 10
    EXCEPT
    SELECT i + 1 FROM foo WHERE i < 5
  )
)
SELECT * FROM foo;

-- this kind of non-linear recursion is not allowed, a recursive term can contain only one recursive reference
WITH RECURSIVE foo AS (
  VALUES (1) AS T(i)
  UNION ALL (
    SELECT i + 1 FROM foo WHERE i < 10
    INTERSECT
    SELECT i + 1 FROM foo WHERE i < 5
  )
)
SELECT * FROM foo;

-- Wrong type induced from non-recursive term
WITH RECURSIVE foo AS (
  VALUES (1), (2) AS T(i)
  UNION ALL
  SELECT CAST((i + 1) AS DECIMAL(10, 0)) FROM foo WHERE i < 10
)
SELECT * FROM foo;

-- rejects different typmod, too (should we allow this?)
WITH RECURSIVE foo AS (
   SELECT CAST(i AS DECIMAL(3, 0)) FROM (VALUES (1), (2)) AS T(i)
   UNION ALL
   SELECT CAST((i + 1) AS DECIMAL(10, 0)) FROM foo WHERE i < 10
)
SELECT * FROM foo;

-- nested recursion
WITH RECURSIVE t AS (
  WITH RECURSIVE s AS (
    VALUES (1) AS T(i)
    UNION ALL
    SELECT i + 1 FROM s WHERE i < 10
  )
  SELECT i AS j FROM s
  UNION ALL
  SELECT j + 1 FROM t WHERE j < 10
)
SELECT * FROM t;

WITH RECURSIVE outermost AS (
  WITH innermost AS (
    SELECT * FROM outermost
  )
  SELECT level + 1 FROM innermost WHERE level < 5
  UNION ALL
  SELECT 0 AS level
)
SELECT * FROM outermost;


--NOT SUPPORTED: alias in CTE declaration

---- sum of 1..100
--WITH RECURSIVE t(n) AS (
--    VALUES (1)
--UNION ALL
--    SELECT n+1 FROM t WHERE n < 100
--)
--SELECT sum(n) FROM t;


--NOT SUPPORTED: RECURSIVE VIEW statements

---- recursive view
--CREATE RECURSIVE VIEW nums (n) AS
--    VALUES (1)
--UNION ALL
--    SELECT n+1 FROM nums WHERE n < 5;
--
--SELECT * FROM nums;
--
--CREATE OR REPLACE RECURSIVE VIEW nums (n) AS
-- DOES NOT WORK
--    VALUES (1)
--UNION ALL
--    SELECT n+1 FROM nums WHERE n < 6;
--
--SELECT * FROM nums;


--NOT SUPPORTED: UNION combinator in recursive CTE

---- This is an infinite loop with UNION ALL, but not with UNION
--WITH RECURSIVE t(n) AS (
--    SELECT 1
--UNION
--    SELECT 10-n FROM t)
--SELECT * FROM t;


--NOT SUPPORTED: LIMIT (infinite recursion is dangerous, limit should be pushed down?)

---- This'd be an infinite loop, but outside query reads only as much as needed
--WITH RECURSIVE t(n) AS (
--    VALUES (1)
--UNION ALL
--    SELECT n+1 FROM t)
--SELECT * FROM t LIMIT 10;


--NOT SUPPORTED: different types of output in anchor and in recursive term

-- POSTGRES: In a perfect world, this would work and resolve the literal as int ...
-- but for now, we have to be content with resolving to text too soon.
--WITH RECURSIVE t AS (
--    SELECT '7' AS n
--UNION ALL
--    SELECT n+1 FROM t WHERE n < 10
--)
--SELECT n FROM t;


--NOT SUPPORTED: WITH can't be nested into FROM as subquery

-- inside subqueries
--SELECT count(*) FROM (
--    WITH RECURSIVE t AS (
--        SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 500
--    )
--    SELECT * FROM t) AS t WHERE n < (
--        SELECT count(*) FROM (
--            WITH RECURSIVE t AS (
--                   SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 100
--                )
--            SELECT * FROM t WHERE n < 50000
--         ) AS t WHERE n < 100);


--NOT SUPPORTED: WITH can't be used with UNION ALL as subquery

---- corner case in which sub-WITH gets initialized first
--with recursive q as (
--      select * from department
--    union all
--      (with x as (select * from q)
--       select * from x)
--    )
--select * from q limit 24;
--
--with recursive q as (
--      select * from department
--    union all
--      (with recursive x as (
--           select * from department
--         union all
--           (select * from q union all select * from x)
--        )
--       select * from x)
--    )
--select * from q limit 32;

--NOT SUPPORTED: forward reference

---- forward reference OK
--WITH RECURSIVE
--    x(id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5),
--    y(id) AS (values (1))
-- SELECT * FROM x;
