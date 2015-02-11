--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: datatypes; Type: TABLE; Schema: public; Owner: -; Tablespace:
--

CREATE TABLE datatypes (
    col1 "char" NOT NULL,
    -- col2 abstime,
    -- col3 aclitem,
    col4 bigint,
    --col6 bit(1),
    -- col7 bit varying,
    col8 boolean,
    col9 bytea,
    col10 character(1),
    col11 character varying,
    --col12 cid,
    --col13 cidr,
    --col14 circle,
    col15 date,
    col16 double precision,
    --col17 gtsvector,
    --col18 inet,
    --col19 int2vector,
    col20 integer,
    --col21 interval,
    --col22 line,
    --col23 lseg,
    --col24 macaddr,
    --col25 money,
    col26 name,
    col27 numeric,
    --col28 oid,
    --col29 oidvector,
    --col30 path,
    --col31 pg_node_tree,
    --col32 point,
    --col33 polygon,
    col34 real,
    --col35 refcursor,
    --col36 regclass,
    --col37 regconfig,
    --col38 regdictionary,
    --col39 regoper,
    --col40 regoperator,
    --col41 regproc,
    --col42 regprocedure,
    --col43 regtype,
    --col44 reltime,
    col46 smallint,
    --col47 smgr,
    col48 text,
    --col49 tid,
    col50 time with time zone,
    col51 time without time zone,
    col52 timestamp with time zone,
    col53 timestamp without time zone
    --col54 tinterval,
    --col55 tsquery,
    --col56 tsvector,
    --col57 txid_snapshot,
    --col58 uuid,
    --col59 xid,
    --col60 xml
);

--
-- Data for Name: datatypes; Type: TABLE DATA; Schema: public; Owner: -
--

-- ROW with all columns filled
INSERT INTO datatypes(col1) VALUES('a');

--col4 bigint
UPDATE datatypes SET col4 = 123 WHERE col1 = 'a';

--col8 boolean
UPDATE datatypes SET col8 = TRUE WHERE col1 = 'a';

--col9 bytea
UPDATE datatypes SET col9 = (decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex')) WHERE col1 = 'a';

--col10 character(1)
UPDATE datatypes SET col10 = 'a' WHERE col1 = 'a';

--col11 character varying
UPDATE datatypes SET col11 = 'abc' WHERE col1 = 'a';

--col15 date
--UPDATE datatypes SET col15 = '5874897-01-01' WHERE col1 = 'a';
UPDATE datatypes SET col15 = '2015-01-01' WHERE col1 = 'a';

--col16 double precision
UPDATE datatypes SET col16 = 0.123456789012345 WHERE col1 = 'a';

--col20 integer
UPDATE datatypes SET col20 = 2147483647 WHERE col1 = 'a';

--col26 name
UPDATE datatypes SET col26 = 'abc' WHERE col1 = 'a';

--col27 numeric
UPDATE datatypes SET col27 = 2147483647 WHERE col1 = 'a';

--col34 real
UPDATE datatypes SET col34 = 0.123456 WHERE col1 = 'a';

--col46 smallint
UPDATE datatypes SET col46 = 32767 WHERE col1 = 'a';

--col48 text
UPDATE datatypes SET col48 = 'abc' WHERE col1 = 'a';

--col50 time with time zone
UPDATE datatypes SET col50 = '23:59:59.999 PST' WHERE col1 = 'a';

--col51 time without time zone
UPDATE datatypes SET col51 = '23:59:59.999' WHERE col1 = 'a';

--col52 timestamp with time zone
--UPDATE datatypes SET col52 = '294276-01-01 23:59:59.999+8' WHERE col1 = 'a';
UPDATE datatypes SET col52 = '2015-01-01 23:59:59.999+8' WHERE col1 = 'a';

--col53 timestamp without time zone
--UPDATE datatypes SET col53 = '294276-01-01 23:59:59.999' WHERE col1 = 'a';
UPDATE datatypes SET col53 = '2015-01-01 23:59:59.999' WHERE col1 = 'a';

-- TODO test max and min numbers

-- ROW with NULLs
INSERT INTO datatypes(col1) VALUES('b');


--COPY datatypes (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col29, col30, col31, col32, col33, col34, col35, col36, col37, col38, col39, col40, col41, col42, col43, col44, col45, col46, col47, col48, col49, col50, col51, col52, col53, col54, col55, col56, col57, col58, col59, col60, col61, col62, col63, col64, col65, col66, col67, col68, col69, col70, col71, col72, col73, col74, col75, col76, col77, col78, col79, col80, col81, col82, col83, col84, col85, col86, col87, col88, col89, col90, col91, col92, col93, col94, col95, col96, col97, col98, col99, col100, col101, col102, col103, col104, col105, col106, col107, col108, col109, col110, col111, col112, col113, col114, col115, col116, col117) FROM stdin;
--a	{a,b,c}	1901-12-14 23:59:59-00:36:45	{"1901-12-14 23:59:59-00:36:45"}	\N	\N	922337203685477580	\N	9223372036854775807	1	1	{abc}	{1,0}	t	{t,f}	\N	\N	a	abc	{abc}	{a}	\N	\N	\N	\N	<(1,1),1>	{"<(1,1),1>"}	2015-01-01	{5874897-01-01,0205-12-31}	0.123456789012340001	{0.123456789012340001,0.123456789012340001}	\N	\N	127.0.0.1	{127.0.0.1}	\N	\N	2147483647	{-2147483648,2147483647}	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	1	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
--b	\N	\N	\N	\N	\N	\N	\N	2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
--\.


ALTER TABLE ONLY datatypes
    ADD CONSTRAINT "Primary key" PRIMARY KEY (col1);

--
-- PostgreSQL database dump complete
--
