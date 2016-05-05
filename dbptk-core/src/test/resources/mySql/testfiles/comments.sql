USE dpttest;

CREATE TABLE test_comments (ID INT COMMENT 'this is an integer') COMMENT 'this is a table';
# SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '' AND TABLE_NAME = 'test_comments';
# SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '' AND TABLE_NAME = '' AND COLUMN_NAME = '';

# dummy (and simple to convert) value so dbptk includes the table in the conversion
INSERT INTO test_comments VALUE (1);
