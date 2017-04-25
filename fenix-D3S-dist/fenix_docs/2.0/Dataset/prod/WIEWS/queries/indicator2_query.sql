with raw as (SELECT
               A.questionid,
               A.approved,
               A.country_id,
               A.iteration,
               subquestionid,
               answerid,
               answer_freetext,
               reference_id,
               a.orgid
             FROM
               answer a
               JOIN
               answer_detail ad
                 ON ( a.id = answerid )
             WHERE
               questionid = 2 ),

    iteration_avg as (
      SELECT
        *,
        (DATE_PART('year', end_date) - DATE_PART('year', start_date) +
         (DATE_PART('month', end_date) - DATE_PART('month', start_date)+1)/12) as avg
      FROM
        iteration
  ),

    answer_1_2 AS (
      SELECT
        iteration,
        iso           AS country_iso3,
        species,
        varieties,
        spec.answerid AS answer_id
      FROM
        ( SELECT
            answerid,
            answer_freetext AS species,
            iteration,
            country_id
          FROM
            raw
          WHERE
            subquestionid = 1003 ) spec
        JOIN

        ( SELECT
            answerid,
            answer_freetext :: INT AS varieties
          FROM
            raw
          WHERE
            subquestionid = 1005 ) var
          ON ( spec.answerid = var.answerid )
        JOIN
        ref_country
          ON ( ref_country.country_id = spec.country_id )
      ORDER BY
        country_iso3,
        species
  ),
    total_country as (SELECT
                        '2_1'                                            AS indicator,
                        iteration,
                        country_iso3                                     AS country_iso3,
                        country_iso3                                     AS wiews_region,
                        1                                                AS rank,
                        count (*)                                        AS value,
                        ( count (*)/b.avg)::REAL                        as avg,
                        'num'                                            AS um
                      FROM
                        answer_1_2 a JOIN  iteration_avg b on (a.iteration = b.current_iteration)
                      GROUP BY
                        iteration,
                        country_iso3,
                        b.avg

                      UNION

                      SELECT
                        '2_2'                                            AS indicator,
                        iteration,
                        country_iso3                                     AS country_iso3,
                        country_iso3                                     AS wiews_region,
                        1                                                AS rank,
                        sum (varieties)                                AS value,
                        (  sum (varieties)/b.avg)::REAL                        as avg,
                        'num'                                            AS um
                      FROM
                        answer_1_2 a JOIN  iteration_avg b on (a.iteration = b.current_iteration)
                      GROUP BY
                        iteration,
                        country_iso3,
                        b.avg


  )

  SELECT * from (
SELECT
  '1130' AS domain,
  indicator,
  iteration,
  'ind_t' as element,
  country_iso3,
  wiews_region,
  rank,
  VALUE,
    'num' as um
FROM
  total_country

GROUP BY
  INDICATOR ,
  iteration,
  country_iso3,
  wiews_region,
  rank,
  VALUE


UNION


SELECT
  '1130' AS domain,
  indicator,
  iteration,
  'ind_a' as element,
  country_iso3,
  wiews_region,
  rank,
  avg as VALUE ,
  'per' as um
FROM
  total_country

GROUP BY
  INDICATOR ,
  iteration,
  country_iso3,
  wiews_region,
  rank,
  avg)z
WHERE country_iso3 = 'ALB'

UNION
    SELECT
    '1130'::text                AS domain,
    indicator_id::text AS indicator,
    iteration::text,
    'nfp'::text as element,

    cast (iso AS TEXT)          AS country,
    cast (iso AS TEXT)          AS wiews_region,
    1::INTEGER                  AS rank,
    nfp_rating                  AS value,
    'per'::text                   AS um
  FROM
    indicator_analysis spec
    JOIN
    ref_country
      ON ( ref_country.country_id = spec.country_id )
  WHERE
    indicator_id = 2




