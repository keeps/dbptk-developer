-- create simple test table
CREATE TABLE public.fstTable (col1 integer);
INSERT INTO public.fstTable (col1) VALUES (1);
INSERT INTO public.fstTable (col1) VALUES (2);

-- test case sensitiveness in table names
CREATE TABLE public.fsttable (col1 integer);
INSERT INTO public.fsttable (col1) VALUES (10);
INSERT INTO public.fsttable (col1) VALUES (20);
