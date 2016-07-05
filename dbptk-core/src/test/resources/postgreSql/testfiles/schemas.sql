-- create simple test table
CREATE TABLE public.fstTable (col1 integer);
INSERT INTO public.fstTable (col1) VALUES (1);
INSERT INTO public.fstTable (col1) VALUES (2);

-- create other schema with a table with the same name
CREATE SCHEMA otherSchema;
CREATE TABLE otherSchema.fstTable (col1 integer);
INSERT INTO otherSchema.fstTable (col1) VALUES (3);
INSERT INTO otherSchema.fstTable (col1) VALUES (4);
