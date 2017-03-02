CREATE OR REPLACE FUNCTION create_priority_analysis()
  RETURNS void AS
$BODY$DROP TABLE IF EXISTS priority_analysis;
CREATE TABLE priority_analysis AS (
	 select
	 recipientcode,
	 donorcode,
	 purposecode,
	 year,
	 sum(value) as value,
	 max(unitcode) as unitcode
	 from usd_commitment
	 group by
	 recipientcode,
	 donorcode,
	 purposecode,
	 year);$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION create_priority_analysis()
  OWNER TO fenix;