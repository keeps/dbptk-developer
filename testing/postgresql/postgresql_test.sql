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
    col2 "char"[],
    col3 abstime,
    col4 abstime[],
    col5 aclitem,
    col6 aclitem[],
    col7 bigint,
    col8 bigint[],
    col9 bigint NOT NULL,
    col10 bit(1),
    col11 bit varying,
    col12 character varying[],
    col13 bit(1)[],
    col14 boolean,
    col15 boolean[],
    col16 bytea,
    col17 bytea[],
    col18 character(1),
    col19 character varying,
    col20 character varying[],
    col21 character(1)[],
    col22 cid,
    col23 cid[],
    col24 cidr,
    col25 cidr[],
    col26 circle,
    col27 circle[],
    col29 date,
    col30 date[],
    col31 double precision,
    col32 double precision[],
    col33 gtsvector,
    col34 gtsvector[],
    col35 inet,
    col36 inet[],
    col37 int2vector,
    col38 int2vector[],
    col39 integer,
    col40 integer[],
    col41 interval,
    col42 interval[],
    col43 line,
    col44 line[],
    col45 lseg,
    col46 lseg[],
    col47 macaddr,
    col48 macaddr[],
    col49 money,
    col50 money[],
    col51 name,
    col52 name[],
    col53 numeric,
    col54 numeric[],
    col55 oid,
    col56 oid[],
    col57 oidvector,
    col58 oidvector[],
    col59 path,
    col60 path[],
    col61 pg_node_tree,
    col62 point,
    col63 point[],
    col64 polygon,
    col65 polygon[],
    col66 real,
    col67 real[],
    col68 refcursor,
    col69 refcursor[],
    col70 regclass,
    col71 regclass[],
    col72 regconfig,
    col73 regconfig[],
    col74 regdictionary,
    col75 regdictionary[],
    col76 regoper,
    col77 regoper[],
    col78 regoperator,
    col79 regoperator[],
    col80 regproc,
    col81 regproc[],
    col82 regprocedure,
    col83 regprocedure[],
    col84 regtype,
    col85 regtype[],
    col86 reltime,
    col87 reltime[],
    col88 integer NOT NULL,
    col89 smallint,
    col90 smallint[],
    col91 smgr,
    col92 text,
    col93 text[],
    col94 tid,
    col95 tid[],
    col96 time with time zone,
    col97 time with time zone[],
    col98 time without time zone,
    col99 time without time zone[],
    col100 timestamp with time zone,
    col101 timestamp with time zone[],
    col102 timestamp without time zone,
    col103 timestamp without time zone[],
    col104 tinterval,
    col105 tinterval[],
    col106 tsquery,
    col107 tsquery[],
    col108 tsvector,
    col109 tsvector[],
    col110 txid_snapshot,
    col111 txid_snapshot[],
    col112 uuid,
    col113 uuid[],
    col114 xid,
    col115 xid[],
    col116 xml,
    col117 xml[]
);


--
-- Name: datatypes_col88_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE datatypes_col88_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: datatypes_col88_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE datatypes_col88_seq OWNED BY datatypes.col88;


--
-- Name: datatypes_col9_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE datatypes_col9_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: datatypes_col9_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE datatypes_col9_seq OWNED BY datatypes.col9;


--
-- Name: col9; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY datatypes ALTER COLUMN col9 SET DEFAULT nextval('datatypes_col9_seq'::regclass);


--
-- Name: col88; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY datatypes ALTER COLUMN col88 SET DEFAULT nextval('datatypes_col88_seq'::regclass);


--
-- Data for Name: datatypes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY datatypes (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col29, col30, col31, col32, col33, col34, col35, col36, col37, col38, col39, col40, col41, col42, col43, col44, col45, col46, col47, col48, col49, col50, col51, col52, col53, col54, col55, col56, col57, col58, col59, col60, col61, col62, col63, col64, col65, col66, col67, col68, col69, col70, col71, col72, col73, col74, col75, col76, col77, col78, col79, col80, col81, col82, col83, col84, col85, col86, col87, col88, col89, col90, col91, col92, col93, col94, col95, col96, col97, col98, col99, col100, col101, col102, col103, col104, col105, col106, col107, col108, col109, col110, col111, col112, col113, col114, col115, col116, col117) FROM stdin;
a	{a,b,c}	1901-12-14 23:59:59-00:36:45	{"1901-12-14 23:59:59-00:36:45"}	\N	\N	922337203685477580	\N	9223372036854775807	1	1	{abc}	{1,0}	t	{t,f}	\N	\N	a	abc	{abc}	{a}	\N	\N	\N	\N	<(1,1),1>	{"<(1,1),1>"}	2015-01-01	{5874897-01-01,0205-12-31}	0.123456789012340001	{0.123456789012340001,0.123456789012340001}	\N	\N	127.0.0.1	{127.0.0.1}	\N	\N	2147483647	{-2147483648,2147483647}	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	1	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
b	\N	\N	\N	\N	\N	\N	\N	2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Name: datatypes_col88_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('datatypes_col88_seq', 2, true);


--
-- Name: datatypes_col9_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('datatypes_col9_seq', 2, true);


--
-- Name: Primary key; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY datatypes
    ADD CONSTRAINT "Primary key" PRIMARY KEY (col1);


--
-- Name: public; Type: ACL; Schema: -; Owner: -
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

