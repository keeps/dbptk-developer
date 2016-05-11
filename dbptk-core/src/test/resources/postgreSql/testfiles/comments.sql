-- tests descriptive metadata supported by postgresql

-- issue: https://github.com/keeps/db-preservation-toolkit/issues/174
-- ✓ COLUMN
-- ✓ CONSTRAINT
-- ✓ DATABASE
--   FOREIGN TABLE
--   FUNCTION
-- ✓ INDEX
--   ROLE
-- ✓ SCHEMA
-- ✓ TABLE
--   TRIGGER
-- ✓ TYPE
-- ✓ VIEW
--   COLLATION (useful?)
--   SERVER (useful?)
--   TABLESPACE (useful?)

-- DATABASE
COMMENT ON DATABASE dpttest IS 'This is the database';

-- SCHEMA
COMMENT ON SCHEMA public IS 'This is the schema';

-- TYPE & (type) COLUMN
CREATE TYPE type1 AS (f1 integer, f2 integer);
COMMENT ON TYPE type1 IS 'This is the first table.';
COMMENT ON COLUMN type1.f1 IS 'This is the first UDT column, f1.';
COMMENT ON COLUMN type1.f2 IS 'This is the second UDT column, f2.';


-- TABLE & (table) COLUMN & INDEX
CREATE TABLE table1 (thecolumn integer CONSTRAINT keyname PRIMARY KEY , theothercolumn type1);
CREATE UNIQUE INDEX table1_index ON table1 (thecolumn);

COMMENT ON TABLE table1 IS 'This is the first table.';
COMMENT ON COLUMN table1.thecolumn IS 'This is the primary key column in the first table.';
COMMENT ON INDEX table1_index IS 'This is an index.';
COMMENT ON CONSTRAINT keyname ON table1 IS 'This is the primary key constraint';

-- VIEW
CREATE VIEW view1 AS
  SELECT thecolumn
  FROM table1
  WHERE thecolumn < 2;
COMMENT ON VIEW view1 IS 'This is a view.';

-- simple values so dbptk includes the table in the conversion
INSERT INTO table1 VALUES (1);
INSERT INTO table1 VALUES (2, ROW(2,4));
INSERT INTO table1 VALUES (3, '(3,6)');


