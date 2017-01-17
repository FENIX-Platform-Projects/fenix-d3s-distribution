CREATE OR REPLACE FUNCTION create_priorities_table()
  RETURNS void AS
$BODY$
DECLARE
   BEGIN
         EXECUTE 'DROP TABLE IF EXISTS combined_priorities_table';
         EXECUTE 'CREATE TABLE combined_priorities_table AS (
            (SELECT ''recipient'' AS typecode, ''UNDAF'' as source, recipientcode, null as donorcode, purposecode from recipient_undaf_priorities GROUP BY recipientcode, purposecode)
             UNION ALL
    (SELECT ''fao'' AS typecode, ''CPF'' as source, recipientcode,  null as donorcode, purposecode from fao_cpf_priorities GROUP BY recipientcode, purposecode)
            UNION ALL
            (SELECT ''partner'' AS typecode, ''Derived priorities'' as source, null as recipientcode, donorcode, purposecode from partner_derived_priorities GROUP BY donorcode, purposecode)
         )';
   END;