-- command
CREATE SCHEMA IF NOT EXISTS DATA;
-- command
CREATE TABLE IF NOT EXISTS Metadata (
  id VARCHAR PRIMARY KEY,
  status VARCHAR,
  rowsCount BIGINT,
  lastUpdate TIMESTAMP,
  timeout TIMESTAMP
);
-- command
CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;
-- command
CREATE AGGREGATE public.FIRST (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);
-- command
CREATE OR REPLACE FUNCTION public.last_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;
-- command
CREATE AGGREGATE public.LAST (
        sfunc    = public.last_agg,
        basetype = anyelement,
        stype    = anyelement
);
-- command
DROP TABLESPACE d3s_tmp;
CREATE TABLESPACE d3s_tmp LOCATION '/tmp/ramdisk/postgres';