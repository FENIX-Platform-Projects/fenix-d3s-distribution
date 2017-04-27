
SELECT

*
from (
SELECT
  a.*,
  ad.*,

  d.crop_name,
  c.wiews_instcode,
  ref_c.iso,
  ref_c.name
FROM   answer a
  JOIN   answer_detail ad
    ON     (a.id = answerid )
  JOIN ref_instab c on a.orgid = c.id
  JOIN ref_country ref_c on ref_c.country_id = a.country_id
  FULL OUTER JOIN  ref_crop d on reference_id = crop_id::TEXT
WHERE  questionid = 11
AND subquestionid =1058
and ref_c.lang = 'EN'
  and d.lang = 'EN'
ORDER BY iso )h
WHERE wiews_instcode = 'ALB026'

UNION


SELECT
*
from (
       SELECT
         a.*,
         ad.*,

         null::TEXT as cropname,
         c.wiews_instcode,
         ref_c.iso,
         ref_c.name
       FROM   answer a
         JOIN   answer_detail ad
           ON     (a.id = answerid )
         JOIN ref_instab c on a.orgid = c.id
         JOIN ref_country ref_c on ref_c.country_id = a.country_id
       WHERE  questionid = 11
              AND subquestionid =1058
              and ref_c.lang = 'EN'
         and ad.reference_id IS null
       ORDER BY iso )h
WHERE wiews_instcode = 'ALB026'

