CREATE TABLE datatypes (
  -- Numeric types
  col1 TINYINT,
  col2 SMALLINT,
  col3 MEDIUMINT,
  col4 INT,
  col5 BIGINT,
  col6 DECIMAL,
  col7 NUMERIC,
  col8 FLOAT(20,10),
  col9 DOUBLE(50,10),
  col10 BIT(1),
  col11 BIT(64),

  -- Date and time types
  col12 DATE,
  col13 DATETIME,
  col14 TIMESTAMP,
  col15 YEAR(4),

  -- String types
  col16 CHAR(255),
  col17 VARCHAR(1024),
  col18 BINARY(255),
  col19 VARBINARY(1024),
  col20 TINYBLOB,
  col21 BLOB,
  col22 MEDIUMBLOB,
  col23 LONGBLOB,
  col24 TINYTEXT,
  col25 TEXT,
  col26 MEDIUMTEXT,
  col27 LONGTEXT,
  col28 ENUM('small','medium','large'),
  col29 SET('one','two','three')

  -- XXX: Spatial data types not added
);

-- ROW with all columns filled


-- Numeric types
-- col1 TINYINT,
INSERT INTO datatypes(col1) VALUES(1);

-- col2 SMALLINT,
UPDATE datatypes SET col2 = 123 WHERE col1 = 1;

-- col3 MEDIUMINT,
UPDATE datatypes SET col3 = 123 WHERE col1 = 1;

-- col4 INT,
UPDATE datatypes SET col4 = 123 WHERE col1 = 1;

-- col5 BIGINT,
UPDATE datatypes SET col5 = 123 WHERE col1 = 1;

-- col6 DECIMAL,
UPDATE datatypes SET col6 = 123 WHERE col1 = 1;

-- col7 NUMERIC,
UPDATE datatypes SET col7 = 123 WHERE col1 = 1;

-- col8 FLOAT(23,10),
UPDATE datatypes SET col8 = 12345678901234.56789 WHERE col1 = 1;

-- col9 DOUBLE(53,10),
UPDATE datatypes SET col9 = 1234567890123456789012345678901234567890123.456789 WHERE col1 = 1;

-- col10 BIT(1),
UPDATE datatypes SET col10 = b'1' WHERE col1 = 1;

-- col11 BIT(64),
UPDATE datatypes SET col11 = b'10101010101010101010101010101010101010101010101010101010101' WHERE col1 = 1;

-- Date and time types
-- col12 DATE,
UPDATE datatypes SET col12 = '9999-12-31' WHERE col1 = 1;

-- col13 DATETIME,
UPDATE datatypes SET col13 = '9999-12-31 23:59:59.999999' WHERE col1 = 1;

-- col14 TIMESTAMP,
UPDATE datatypes SET col14 = '2038-01-19 03:14:07.999999' WHERE col1 = 1;

-- col15 YEAR(4),
UPDATE datatypes SET col15 = '2015' WHERE col1 = 1;


-- String types
-- col16 CHAR(255),
-- col17 VARCHAR(1024),
-- col18 BINARY(255),
-- col19 VARBINARY(1024),
-- col20 TINYBLOB,
-- col21 BLOB,
-- col22 MEDIUMBLOB,
-- col23 LONGBLOB,
-- col24 TINYTEXT,
-- col25 TEXT,
-- col26 MEDIUMTEXT,
-- col27 LONGTEXT,
-- col28 ENUM('small','medium','large'),
-- col29 SET('one','two','three')

-- TODO test max and min numbers

-- ROW with NULLs
INSERT INTO datatypes(col1) VALUES(2);


ALTER TABLE datatypes
    ADD CONSTRAINT Primary_key PRIMARY KEY (col1);
