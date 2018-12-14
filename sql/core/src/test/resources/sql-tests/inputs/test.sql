-- Test data
CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
  (1, true), (1, false),
  (2, true),
  (3, false), (3, null),
  (4, null), (4, null),
  (5, null), (5, true), (5, false) AS test_agg(k, v);

-- simple explain of queries having every/some/any agregates. Optimized
-- plan should show the rewritten aggregate expression.
EXPLAIN EXTENDED SELECT k, every(v), some(v), any(v) FROM test_agg GROUP BY k;
