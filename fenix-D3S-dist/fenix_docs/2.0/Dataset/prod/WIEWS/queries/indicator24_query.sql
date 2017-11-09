DROP TABLE IF EXISTS indicators.indicator24;

CREATE TABLE indicators.indicator24 as (

  WITH raw AS (
      SELECT
        a.questionid,
        a.id                                       AS answer_id,
        a.iteration :: TEXT,
        c.iso                                      AS country_iso3,
        it.wiews_instcode                          AS stakeholder,
        a.datasource,
        a.created_by,
        a.created_date :: TEXT,
        a.modified_by,
        a.modified_date :: TEXT,
        coalesce(adn.answer_freetext, '0') :: REAL AS accessions_regeneration,
        coalesce(add.answer_freetext, '0') :: REAL AS accessions_num
      FROM answer a
        JOIN answer_detail adn ON (adn.subquestionid = 1089 AND a.id = adn.answerid)
        JOIN answer_detail add ON (add.subquestionid = 1087 AND a.id = add.answerid)
        JOIN ref_country c ON (a.country_id = c.country_id)
        JOIN ref_instab it ON (it.id = a.orgId)
      WHERE a.approved = 1
  ),
      stakeholder_raw AS (
        SELECT
          iteration,
          country_iso3,
          stakeholder,
          sum(accessions_regeneration)                                        AS accessions_regeneration,
          sum(accessions_num)                                                  AS accessions_num,
          CASE WHEN sum(accessions_num) = 0
            THEN 0
          ELSE (sum(accessions_regeneration) / sum(accessions_num)) * 100 END AS value
        FROM raw
        GROUP BY iteration, country_iso3, stakeholder
    ),
      country_raw AS (
        SELECT
          iteration,
          country_iso3,
          sum(accessions_regeneration)                                        AS accessions_regeneration,
          sum(accessions_num)                                                  AS accessions_num,
          CASE WHEN sum(accessions_num) = 0
            THEN 0
          ELSE (sum(accessions_regeneration) / sum(accessions_num)) * 100 END AS value
        FROM stakeholder_raw
        GROUP BY iteration, country_iso3
    ),
      nfp_raw AS (
        SELECT
          c.iso              AS country_iso3,
          iteration :: TEXT,
          nfp_rating :: REAL AS value
        FROM indicator_analysis spec
          JOIN ref_country c ON (c.country_id = spec.country_id)
        WHERE indicator_id = 22 AND nfp_rating>0
    )

  --STAKEHOLDER
  SELECT
    '2310' :: TEXT AS domain,
    country_iso3   AS wiews_region,
    stakeholder,
    '24' :: TEXT   AS indicator,
    'stk' :: TEXT  AS element,
    iteration :: TEXT,
    value,
    'per' :: TEXT  AS um,
    accessions_regeneration,
    accessions_num,
    country_iso3,
    1 :: INTEGER   AS rank
  FROM stakeholder_raw
  --COUNTRY
  UNION
  SELECT
    '2310' :: TEXT AS domain,
    country_iso3   AS wiews_region,
    'na'           AS stakeholder,
    '24' :: TEXT   AS indicator,
    'ind' :: TEXT  AS element,
    iteration :: TEXT,
    value,
    'per' :: TEXT  AS um,
    accessions_regeneration,
    accessions_num,
    country_iso3,
    1 :: INTEGER   AS rank
  FROM country_raw
  --REGION
  UNION
  SELECT
    '2310' :: TEXT                                                       AS domain,
    w                                                                    AS wiews_region,
    'na'                                                                 AS stakeholder,
    '24' :: TEXT                                                         AS indicator,
    'ind' :: TEXT                                                        AS element,
    iteration :: TEXT,
    CASE WHEN sum(accessions_num) = 0
      THEN 0
    ELSE (sum(accessions_regeneration) / sum(accessions_num)) * 100 END AS value,
    'per' :: TEXT                                                        AS um,
    sum(accessions_regeneration)                                        AS accessions_regeneration,
    sum(accessions_num)                                                  AS accessions_num,
    'na'                                                                 AS country_iso3,
    rank
  FROM country_raw i
    JOIN codelist.ref_region_country r ON (i.country_iso3 = r.country_iso3)
  GROUP BY iteration, w, rank
  --NFP
  UNION
  SELECT
    '2310' :: TEXT AS domain,
    country_iso3   AS wiews_region,
    'na'           AS stakeholder,
    '24' :: TEXT   AS indicator,
    'nfp' :: TEXT  AS element,
    iteration :: TEXT,
    value,
    'num' :: TEXT  AS um,
    NULL           AS accessions_regeneration,
    NULL           AS accessions_num,
    country_iso3,
    1 :: INTEGER   AS rank
  FROM nfp_raw
  --REGION NFP
  UNION
  SELECT
    '2310' :: TEXT AS domain,
    w              AS wiews_region,
    'na'           AS stakeholder,
    '24' :: TEXT   AS indicator,
    'nfpa' :: TEXT AS element,
    iteration :: TEXT,
    avg(value)     AS value,
    'num' :: TEXT  AS um,
    NULL           AS accessions_regeneration,
    NULL           AS accessions_num,
    'na'           AS country_iso3,
    rank
  FROM nfp_raw i
    JOIN codelist.ref_region_country r ON (i.country_iso3 = r.country_iso3)
  GROUP BY iteration, w, rank

  ORDER BY rank, wiews_region, element

);