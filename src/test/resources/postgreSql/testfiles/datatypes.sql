
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


ALTER TABLE ONLY datatypes
    ADD CONSTRAINT "Primary key" PRIMARY KEY (col1);

