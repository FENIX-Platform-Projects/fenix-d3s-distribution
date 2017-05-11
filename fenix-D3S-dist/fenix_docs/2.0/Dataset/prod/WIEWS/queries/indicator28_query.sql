DROP TABLE IF EXISTS indicators.indicator28;

CREATE TABLE indicators.indicator28 as (

WITH raw AS ( SELECT  *
              FROM
                ( SELECT
                    a.iteration,
                    a.id                     AS answer_ID,
                    co.iso                   AS country,
                    it.wiews_instcode        AS stakeholder,
                    c.subquestionid :: TEXT  AS subquestion_id,
                    CASE WHEN c.reference_id IS NOT NULL
                      THEN crop_name
                    ELSE answer_freetext END AS crop
                  FROM
                    answer a
                    FULL OUTER JOIN
                    answer_detail c
                      ON ( c.answerid = a.id AND
                           c.subquestionid IN ( 1102, 1103 ) )
                    FULL OUTER JOIN
                    ref_crop ref
                      ON ( ref.crop_id :: TEXT = c.reference_id :: TEXT AND
                           ref.lang = 'EN' )
                    JOIN
                    ref_instab it
                      ON ( it.id = a.orgId )
                    LEFT JOIN
                    ref_country co
                      ON ( co.country_id = a.country_id ) ) f
              WHERE
                crop IS NOT NULL
),

  iteration_avg AS (
      SELECT
        *,
        ( Date_part ('year', end_date) - Date_part ('year', start_date) +
          ( Date_part ('month', end_date) - Date_part ('month', start_date) + 1 ) / 12 ) AS avg
      FROM
        iteration      ),

  crops AS (
    SELECT
      iteration,
      country,
      stakeholder,
      crop_name,
      sum (crop_num) AS crop_num,
      max (um)       AS um

    FROM
      ( SELECT
          a.iteration             AS iteration,
          a.answer_ID,
          a.country,
          a.stakeholder,
          a.crop                  AS crop_name,
          sum (b.crop :: INTEGER) AS crop_num,
          'num' :: TEXT           AS um
        FROM
          /* Total number of accessions distributed by the national genebank(s) */
          ( SELECT
              *
            FROM
              raw
            WHERE
              subquestion_id = '1102' ) a
          JOIN
          /* Name of crop/crop group */

          ( SELECT
              *
            FROM
              raw
            WHERE
              subquestion_id = '1103' ) b
            ON ( a.answer_id = b.answer_id AND a.iteration = b.iteration AND a.country = b.country AND
                 a.stakeholder = b.stakeholder )
        GROUP BY
          a.iteration,
          a.answer_ID,
          a.country,
          a.stakeholder,
          a.crop
        ORDER BY
          country ) z
    GROUP BY
      iteration,
      country,
      stakeholder,
      crop_name ),


    ind16 AS (
      SELECT
        '3140' :: TEXT AS domain,
        country::TEXT        AS wiews_region,
        country::TEXT,
        stakeholder,
        '28' :: TEXT   AS indicator,
        'crp' :: TEXT  AS element,
        iteration :: TEXT,
        crop_name      AS crop,
        crop_num       AS value,
        um,
        1 :: INTEGER   AS rank
      FROM
        crops
  ),

    nfp AS (
      SELECT
        '3140' :: TEXT         AS domain,
        c.iso::TEXT                  AS wiews_region,
        c.iso::TEXT                  AS country,
        'na' :: TEXT           AS stakeholder,
        '28' :: TEXT           AS indicator,
        'nfp' :: TEXT          AS element,
        spec.iteration :: TEXT AS iteration,
        'na' :: TEXT           AS crop,
        nfp_rating :: REAL     AS value,
        'per' :: TEXT          AS um,
        1 :: INTEGER           AS rank
      FROM
        indicator_analysis spec
        JOIN
        ref_country c
          ON ( c.country_id = spec.country_id )
      WHERE
        indicator_id = 28
  )

/* by crop */
select * from ind16
UNION

/* by nfp */
SELECT * from nfp
union

/* by stakeholder(total) */
SELECT
  domain,
  wiews_region,
  country,
  stakeholder,
  indicator,
  'stk' :: TEXT      AS element,
  iteration,
  'na' :: TEXT       AS crop,
  sum(value) as value,
  um,
  rank
FROM
  ind16
GROUP BY
  domain,
  wiews_region,
  country,
  stakeholder,
  indicator,
  iteration,
  um,
  rank
UNION

/* by country ( total) */
SELECT
  domain,
  wiews_region,
  country,
  'na' :: TEXT       AS stakeholder,
  indicator,
  'ind_t' :: TEXT      AS element,
  iteration,
  'na' :: TEXT       AS crop,
  sum(value) as value,
  um,
  rank
FROM
  ind16
GROUP BY
  domain,
  wiews_region,
  country,
  indicator,
  iteration,
  um,
  rank
UNION

/* by country ( avg) */
SELECT
  domain,
  wiews_region,
  country,
  'na' :: TEXT       AS stakeholder,
  indicator,
  'ind_a' :: TEXT      AS element,
  iteration,
  'na' :: TEXT       AS crop,
  (sum(value)/ b.avg) as value,
  um,
  rank
FROM
  ind16 a
  JOIN iteration_avg b on (a.iteration = b.current_iteration::TEXT)
GROUP BY
  domain,
  wiews_region,
  country,
  indicator,
  iteration,
  um,
  rank,
  b.avg
UNION

/* by stakeholder (avg) */
SELECT
  domain,
  wiews_region,
  country,
  stakeholder,
  indicator,
  'stk_a' :: TEXT      AS element,
  iteration,
  'na' :: TEXT       AS crop,
  (sum(value)/ b.avg) as value,
  um,
  rank
FROM
  ind16 a
  JOIN iteration_avg b on (a.iteration = b.current_iteration::TEXT)
GROUP BY
  domain,
  wiews_region,
  country,
  stakeholder,
  indicator,
  iteration,
  um,
  rank,
  b.avg);


/* REGIONAL AGGREGATIONS */
INSERT into indicators.indicator16
/* nfp rating average by region */
  SELECT
    domain,
    b.w::TEXT as wiews_region,
    'na'::TEXT as country,
    'na'::TEXT as stakeholder,
    indicator,
    'nfp_a'::TEXT as element,
    iteration,
    'na'::TEXT as crop,
    avg(value) as value,
    'per'::TEXT as um,
    b.rank::INTEGER
  FROM
    (SELECT * from indicators.indicator16 WHERE element='nfp') a  join
    codelist.ref_region_country b
      on a.country = b.country_iso3
  GROUP BY domain, indicator, iteration, b.w, b.rank

  UNION

  /* regional aggregation (TOTAL) for indicator*/
  SELECT
    max(domain) as domain,
    w::TEXT as wiews_region,
    'na' :: TEXT as country,
    'na' :: TEXT as stakeholder,
    max(indicator) as indicator,
    'ind_t' as element,
    iteration,
    'na' :: TEXT as crop,
    sum(value) as value,
    'num'               AS um,
    rank as rank
  FROM ( SELECT
           b.w,
           b.country_iso3,
           b.rank,
           b.label,
           a.indicator,
           a.iteration,
           a.domain,
           a.country,
           a.element,
           a.stakeholder,
           a.crop,
           a.value
         FROM
           codelist.ref_region_country b JOIN (SELECT * from indicators.indicator16 where element='ind_t')a
             ON a.country = b.country_iso3)z
  GROUP BY iteration, wiews_region, rank
  UNION

  /* regional aggregation (AVG) for indicator*/
  SELECT
    max(domain) as domain,
    w::TEXT as wiews_region,
    'na' :: TEXT as country,
    'na' :: TEXT as stakeholder,
    max(indicator) as indicator,
    'ind_t' as element,
    iteration,
    'na' :: TEXT as crop,
    sum(value) as value,
    'num'               AS um,
    rank as rank
  FROM ( SELECT
           b.w,
           b.country_iso3,
           b.rank,
           b.label,
           a.indicator,
           a.iteration,
           a.domain,
           a.country,
           a.element,
           a.stakeholder,
           a.crop,
           a.value
         FROM
           codelist.ref_region_country b JOIN (SELECT * from indicators.indicator16 where element='ind_a')a
             ON a.country = b.country_iso3)z
  GROUP BY iteration, wiews_region, rank;



