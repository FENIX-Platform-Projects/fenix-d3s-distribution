CREATE SCHEMA IF NOT EXISTS DATA;

CREATE TABLE IF NOT EXISTS Metadata (
  id VARCHAR PRIMARY KEY,
  status VARCHAR,
  rowsCount BIGINT,
  lastUpdate TIMESTAMP,
  timeout TIMESTAMP
);

CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;

CREATE AGGREGATE public.FIRST (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);

CREATE OR REPLACE FUNCTION public.last_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;

CREATE AGGREGATE public.LAST (
        sfunc    = public.last_agg,
        basetype = anyelement,
        stype    = anyelement
);


CREATE TABLESPACE d3s_tmp LOCATION '/tmp/ramdisk/postgres';