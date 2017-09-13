DROP TABLE IF EXISTS indicators.indicator2;

CREATE TABLE indicators.indicator2 as (

WITH
    raw AS (
      SELECT a.questionid,
        a.approved,
        a.country_id,
        a.iteration,
        subquestionid,
        answerid,
        answer_freetext,
        reference_id,
        a.orgid
      FROM   answer a
        JOIN   answer_detail ad
          ON     (
          a.id = answerid )
      WHERE  questionid = 2
  ),

    iteration_avg AS (
      SELECT
        *,
        (Date_part('year', end_date) - Date_part('year', start_date) +
         (Date_part('month', end_date) - Date_part('month', start_date)+1) / 12) AS avg
      FROM   iteration
  ),

    answer_1_2 AS (
      SELECT   iteration,
        iso AS country_iso3,
        species,
        varieties,
        spec.answerid AS answer_id
      FROM
        ( SELECT answerid,
            answer_freetext AS species,
            iteration,
            country_id
          FROM   raw
          WHERE  subquestionid = 1003 ) spec
        JOIN (
               SELECT
                 answerid,
                 answer_freetext :: int AS varieties
               FROM   raw
               WHERE  subquestionid = 1005 ) var
          ON (spec.answerid = var.answerid )
        JOIN ref_country ON ( ref_country.country_id = spec.country_id )
      ORDER BY
        country_iso3,
        species
  ),
    total_country AS (
    SELECT
      '2_1'                         AS indicator,
      iteration,
      country_iso3                  AS country_iso3,
      country_iso3                  AS wiews_region,
      1                             AS rank,
      Count (*)                     AS value,
      ( Count (*) / b.avg ) :: REAL AS avg,
      'num'                         AS um
    FROM
      answer_1_2 a

      JOIN iteration_avg b ON ( a.iteration = b.current_iteration )
    GROUP BY
      iteration,
      country_iso3,
      b.avg

    UNION

    SELECT
      '2_2'                               AS indicator,
      iteration,
      country_iso3                        AS country_iso3,
      country_iso3                        AS wiews_region,
      1                                   AS rank,
      sum (varieties)                     AS value,
      ( sum (varieties) / b.avg ) :: REAL AS avg,
      'num'                               AS um
    FROM
      answer_1_2 a
      JOIN iteration_avg b ON ( a.iteration = b.current_iteration )
    GROUP BY
      iteration,
      country_iso3,
      b.avg
  ),

  total_region as (
      SELECT
        '1120'::text AS domain,
         indicator::text,
        iteration::text,
        b.w                                                AS wiews_region,
        b.rank::INTEGER                                             AS rank,
        sum(raw.value)                                     AS value,
        avg(raw.avg)                                       AS avg
      FROM total_country raw join codelist.ref_region_country b on (raw.country_iso3 = b.country_iso3 )
      GROUP BY
      iteration,
        indicator,
        b.rank,
        b.w
  ),
  nfp_rating as (
      SELECT
       '1120'::text            AS domain,
       '2_1'::text        AS indicator,
       iteration::text,
       'nfp'::text        AS element,
       iso::TEXT              AS country,
       iso::TEXT              AS wiews_region,
       1::integer         AS rank,
       nfp_rating         AS value,
       'num'::text        AS um
     FROM   indicator_analysis spec
       JOIN   ref_country ON ( ref_country.country_id = spec.country_id )
     WHERE  indicator_id = 2
  )

/* species and varieties total number from different indicators by country */
SELECT
  '1120' AS domain,
  indicator,
  iteration::text,
  'ind_t' AS element,
  country_iso3,
  wiews_region,
  rank,
  value,
  'num' AS um
FROM     total_country
GROUP BY
  indicator ,
  iteration,
  country_iso3,
  wiews_region,
  rank,
  value

UNION

/* species and varieties average number from different indicators by country */

SELECT
  '1120' AS domain,
  indicator,
  iteration::text,
  'ind_a' AS element,
  country_iso3,
  wiews_region,
  rank,
  avg   AS value ,
  'num' AS um
FROM     total_country
GROUP BY indicator ,
  iteration,
  country_iso3,
  wiews_region,
  rank,
  avg

UNION
/* nfp rating */
SELECT
  domain,
  INDICATOR ,
  iteration,
  element,
  country,
  wiews_region,
  rank,
  CASE WHEN value > 0 THEN VALUE ELSE NULL END as value,
  um
FROM
  nfp_rating

UNION
/* nfp rating average by region */
SELECT
  domain,
  indicator,
  iteration,
  'nfp_a' as element,
  'na' as country,
  b.w as wiews_region,
  b.rank,
  avg(value) as value,
  'num' as um

FROM
  nfp_rating a  join
  codelist.ref_region_country b
    on a.country = b.country_iso3
    WHERE value > 0
GROUP BY domain, indicator, iteration, b.w, b.rank

union
/* species and varieties total number from different indicators by region */

SELECT
  domain,
  indicator,
  iteration,
  'ind_t' as element,
  'na' as country_iso3,
  wiews_region,
  rank,
  value,
  'num' as um
FROM
  total_region
UNION
/* species and varieties total number from different indicators by region */

SELECT
  domain,
  indicator,
  iteration,
  'ind_a' as element,
  'na' as country_iso3,
  wiews_region,
  rank,
  AVG as value,
  'num' as um
FROM
  total_region);
