CREATE TABLE test_comments (ID INT COMMENT 'this is an integer') COMMENT 'this is a table';

# a simple value so dbptk includes the table in the conversion
INSERT INTO test_comments VALUE (1);
