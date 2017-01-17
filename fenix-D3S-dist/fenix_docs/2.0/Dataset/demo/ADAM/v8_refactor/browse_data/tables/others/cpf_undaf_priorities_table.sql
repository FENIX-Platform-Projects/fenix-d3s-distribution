CREATE OR REPLACE FUNCTION create_cpf_undaf_priorities_table()
  RETURNS void AS
$BODY$
DECLARE
   BEGIN
         EXECUTE 'DROP TABLE IF EXISTS cpf_undaf_priorities_table';
         EXECUTE 'CREATE TABLE cpf_undaf_priorities_table AS (

            SELECT * FROM
            (SELECT  recipient_undaf_priorities.recipientcode, recipient_undaf_priorities.purposecode,
            recipient_undaf_priorities.from_year ||''-''|| recipient_undaf_priorities.to_year as undaf_period,
            fao_cpf_priorities.from_year ||''-''|| fao_cpf_priorities.to_year as cpf_period,
            RTRIM(recipient_undaf_priorities.stated_priority) as undaf_stated_priority,
            RTRIM(fao_cpf_priorities.stated_priority) as cpf_stated_priority

            FROM recipient_undaf_priorities
    LEFT OUTER JOIN fao_cpf_priorities
ON recipient_undaf_priorities.recipientcode = fao_cpf_priorities.recipientcode
               AND recipient_undaf_priorities.purposecode = fao_cpf_priorities.purposecode) a

UNION

            (SELECT  fao_cpf_priorities.recipientcode, fao_cpf_priorities.purposecode,
            recipient_undaf_priorities.from_year ||''-''|| recipient_undaf_priorities.to_year as undaf_period,
fao_cpf_priorities.from_year ||''-''|| fao_cpf_priorities.to_year as cpf_period,
            RTRIM(recipient_undaf_priorities.stated_priority) as undaf_stated_priority,
RTRIM(fao_cpf_priorities.stated_priority) as cpf_stated_priority

            FROM fao_cpf_priorities
    LEFT OUTER JOIN recipient_undaf_priorities
ON fao_cpf_priorities.recipientcode = recipient_undaf_priorities.recipientcode
               AND fao_cpf_priorities.purposecode = recipient_undaf_priorities.purposecode)
         )';


        -- remove carriage returns
         EXECUTE 'UPDATE cpf_undaf_priorities_table set undaf_stated_priority =  regexp_replace(undaf_stated_priority, E''[\n\r\u2028]+'','''', ''g'')';
         EXECUTE 'UPDATE cpf_undaf_priorities_table set cpf_stated_priority =  regexp_replace(cpf_stated_priority, E''[\n\r\u2028]+'','''', ''g'')';

   END;