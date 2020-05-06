CREATE TABLE table1 (
  `id` VARCHAR
);

CREATE TABLE table2 (
  `id` VARCHAR,
  `name` VARCHAR
);

CREATE TABLE table3 (
  `id` VARCHAR,
  `name` VARCHAR
);

INSERT INTO table3
SELECT
  t1.`id`, t2.`name`
FROM
  table1 AS t1
INNER JOIN
  table2 AS t2
ON
  t1.`id` = t2.`id`;