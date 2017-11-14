DROP TABLE IF EXISTS indicators.indicator16;

CREATE TABLE indicators.indicator16 as (
  WITH
      itw AS (
      --SELECT version AS iteration, 365.0 / (end_date - start_date) AS year_weight FROM iteration
        SELECT version as iteration, 12.0 / (Date_part('year', end_date) * 12 + Date_part('month', end_date) - Date_part('year', start_date) * 12 + Date_part('month', start_date) - 1) AS year_weight FROM iteration
    ),
      raw AS (
        SELECT
          a.iteration,
          a.id                                                     AS answer_ID,
          co.iso                                                   AS country,
          it.wiews_instcode                                        AS stakeholder,
          ac.answer_freetext :: INTEGER                            AS accessions_number,
          coalesce(ac.answer_freetext :: INTEGER, 0) * year_weight AS accessions_number_avg,
          CASE WHEN cr.reference_id IS NOT NULL
            THEN crop_name
          ELSE cr.answer_freetext END                              AS crop_en
        FROM
          answer a
          JOIN answer_detail ac ON (ac.answerid = a.id AND ac.subquestionid = 1070)
          JOIN ref_instab it ON (it.id = a.orgId)
          JOIN itw ON (itw.iteration = a.iteration)
          LEFT JOIN answer_detail cr ON (cr.answerid = a.id AND cr.subquestionid = 1068)
          LEFT JOIN ref_crop ref ON (ref.crop_id :: TEXT = cr.reference_id :: TEXT AND ref.lang = 'EN')
          LEFT JOIN ref_country co ON (co.country_id = a.country_id)
    ),

      stk AS (
        SELECT
          iteration :: TEXT,
          country :: TEXT,
          stakeholder,
          sum(accessions_number)     AS accessions_number,
          sum(accessions_number_avg) AS accessions_number_avg
        FROM
          raw
        GROUP BY iteration, country, stakeholder
    ),

      country AS (
        SELECT
          iteration :: TEXT,
          country :: TEXT,
          sum(accessions_number)     AS accessions_number,
          sum(accessions_number_avg) AS accessions_number_avg
        FROM
          stk
        GROUP BY iteration, country
    ),

      region AS (
        SELECT
          iteration :: TEXT,
          w                          AS wiews_region,
          sum(accessions_number)     AS accessions_number,
          sum(accessions_number_avg) AS accessions_number_avg,
          rank
        FROM
          country
          JOIN codelist.ref_region_country r ON (country = r.country_iso3)
        GROUP BY iteration, w, rank
    ),

      nfp AS (
        SELECT
          c.iso :: TEXT          AS country,
          spec.iteration :: TEXT AS iteration,
          nfp_rating :: REAL
        FROM
          indicator_analysis spec
          JOIN ref_country c ON (c.country_id = spec.country_id)
        WHERE
          indicator_id = 16 AND nfp_rating > 0
    ),

      nfp_region AS (
        SELECT
          iteration,
          w               AS wiews_region,
          avg(nfp_rating) AS nfp_rating,
          rank
        FROM
          nfp
          JOIN codelist.ref_region_country r ON (country = r.country_iso3)
        GROUP BY iteration, w, rank
    )

  /* by stakeholder */
  SELECT
    '2140' :: TEXT    AS domain,
    country           AS wiews_region,
    country,
    stakeholder,
    '16' :: TEXT      AS indicator,
    'stk_t' :: TEXT   AS element,
    iteration,
    'na' :: TEXT      AS crop,
    accessions_number AS value,
    'num'             AS um,
    1 :: INTEGER      AS rank
  FROM stk
  UNION ALL
  SELECT
    '2140' :: TEXT        AS domain,
    country               AS wiews_region,
    country,
    stakeholder,
    '16' :: TEXT          AS indicator,
    'stk_a' :: TEXT       AS element,
    iteration,
    'na' :: TEXT          AS crop,
    accessions_number_avg AS value,
    'num'                 AS um,
    1 :: INTEGER          AS rank
  FROM stk
  /* by country */
  UNION ALL
  SELECT
    '2140' :: TEXT    AS domain,
    country           AS wiews_region,
    country,
    'na' :: TEXT      AS stakeholder,
    '16' :: TEXT      AS indicator,
    'ind_t' :: TEXT   AS element,
    iteration,
    'na' :: TEXT      AS crop,
    accessions_number AS value,
    'num'             AS um,
    1 :: INTEGER      AS rank
  FROM country
  UNION ALL
  SELECT
    '2140' :: TEXT        AS domain,
    country               AS wiews_region,
    country,
    'na' :: TEXT          AS stakeholder,
    '16' :: TEXT          AS indicator,
    'ind_a' :: TEXT       AS element,
    iteration,
    'na' :: TEXT          AS crop,
    accessions_number_avg AS value,
    'num'                 AS um,
    1 :: INTEGER          AS rank
  FROM country
  /* by region */
  UNION ALL
  SELECT
    '2140' :: TEXT    AS domain,
    wiews_region,
    'na' :: TEXT      AS country,
    'na' :: TEXT      AS stakeholder,
    '16' :: TEXT      AS indicator,
    'ind_t' :: TEXT   AS element,
    iteration,
    'na' :: TEXT      AS crop,
    accessions_number AS value,
    'num'             AS um,
    rank
  FROM region
  UNION ALL
  SELECT
    '2140' :: TEXT        AS domain,
    wiews_region,
    'na' :: TEXT          AS country,
    'na' :: TEXT          AS stakeholder,
    '16' :: TEXT          AS indicator,
    'ind_a' :: TEXT       AS element,
    iteration,
    'na' :: TEXT          AS crop,
    accessions_number_avg AS value,
    'num'                 AS um,
    rank
  FROM region
  /* by nfp */
  UNION ALL
  SELECT
    '2140' :: TEXT AS domain,
    country        AS wiews_region,
    country,
    'na' :: TEXT   AS stakeholder,
    '16' :: TEXT   AS indicator,
    'nfp' :: TEXT  AS element,
    iteration,
    'na' :: TEXT   AS crop,
    nfp_rating     AS value,
    'num'          AS um,
    1 :: INTEGER   AS rank
  FROM nfp
  UNION ALL
  SELECT
    '2140' :: TEXT AS domain,
    wiews_region,
    'na' :: TEXT   AS country,
    'na' :: TEXT   AS stakeholder,
    '16' :: TEXT   AS indicator,
    'nfpa' :: TEXT AS element,
    iteration,
    'na' :: TEXT   AS crop,
    nfp_rating     AS value,
    'num'          AS um,
    rank
  FROM nfp_region
);