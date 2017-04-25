DROP TABLE IF EXISTS indicators.indicator3;

CREATE TABLE indicators.indicator3 as (

  WITH raw AS (SELECT a.* FROM
    ( SELECT
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
        questionid = 2 )a
    JOIN
    ref_instab d
      ON d.ID = a.orgId
  WHERE
    d.wiews_instcode NOT IN
    ( 'BEL084', 'CIV033', 'COL003', 'ETH013', 'IND002', 'KEN056', 'MEX002', 'NGA039', 'PER001', 'PHL001', 'SYR002', 'TWN001', 'FJI049', 'SWE054' )
  ),

      raw_witc as (SELECT
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

      answer_1_2_witc AS (
        SELECT
          iteration,
          iso           AS country_iso3,
          species,
          threatened,
          varieties,
          threatened_varieties,
          spec.answerid AS answer_id
        FROM
          ( SELECT
              answerid,
              answer_freetext AS species,
              iteration,
              country_id
            FROM
              raw_witc
            WHERE
              subquestionid = 1003 ) spec
          JOIN
          ( SELECT
              answerid,
              CASE WHEN reference_id = '1001'
                THEN TRUE
              ELSE FALSE END AS threatened
            FROM
              raw_witc
            WHERE
              subquestionid = 1004 ) tspec
            ON ( spec.answerid = tspec.answerid )
          JOIN
          ( SELECT
              answerid,
              answer_freetext :: INT AS varieties
            FROM
              raw_witc
            WHERE
              subquestionid = 1005 ) var
            ON ( spec.answerid = var.answerid )
          JOIN
          ( SELECT
              answerid,
              answer_freetext :: INT AS threatened_varieties
            FROM
              raw_witc
            WHERE
              subquestionid = 1006 ) tvar
            ON ( spec.answerid = tvar.answerid )
          JOIN
          ref_country
            ON ( ref_country.country_id = spec.country_id )
        ORDER BY
          country_iso3,
          species
    ),


      answer_1_2 AS (
        SELECT
          iteration,
          iso           AS country_iso3,
          species,
          threatened,
          varieties,
          threatened_varieties,
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
              CASE WHEN reference_id = '1001'
                THEN TRUE
              ELSE FALSE END AS threatened
            FROM
              raw
            WHERE
              subquestionid = 1004 ) tspec
            ON ( spec.answerid = tspec.answerid )
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
          ( SELECT
              answerid,
              answer_freetext :: INT AS threatened_varieties
            FROM
              raw
            WHERE
              subquestionid = 1006 ) tvar
            ON ( spec.answerid = tvar.answerid )
          JOIN
          ref_country
            ON ( ref_country.country_id = spec.country_id )
        ORDER BY
          country_iso3,
          species
    ), total AS (

    SELECT
      '3_1'                                              AS indicator,
      iteration,
      country_iso3,
      country_iso3                                       AS wiews_region,
      1                                                  AS rank,
      count (*)                                          AS species_number,
      sum (threatened_inc)                               AS threatened_species_number,
      ( sum (threatened_inc) :: REAL / count (*) ) * 100 AS threatened_percentage,
      'per'                                                AS um
    FROM
      ( SELECT
          *,
          CASE WHEN threatened
            THEN 1
          ELSE 0 END AS threatened_inc
        FROM
          answer_1_2 ) raw
    GROUP BY
      iteration,
      country_iso3
    UNION
    SELECT
      '3_2'                                                          AS indicator,
      iteration,
      country_iso3,
      country_iso3                                                   AS wiews_region,
      1                                                              AS rank,

      sum (varieties)                                                AS varieties_number,
      sum (threatened_varieties)                                     AS threatened_varieties_number,
      ( sum (threatened_varieties) :: REAL / sum (varieties) ) * 100 AS threatened_percentage,
      'per'                                                            AS um
    FROM
      ( SELECT
          *,
          CASE WHEN threatened
            THEN 1
          ELSE 0 END AS threatened_inc
        FROM
          answer_1_2 ) raw
    GROUP BY
      iteration,
      country_iso3
    HAVING sum (varieties) > 0
    ORDER BY
      indicator,
      iteration,
      country_iso3),


      total_witc as (

      SELECT
        '3_1'                                              AS indicator,
        iteration,
        'na'                                               AS country_iso3,
        'WITC'                                             AS wiews_region,
        500                                                  AS rank,
        count (*)                                          AS species_number,
        sum (threatened_inc)                               AS threatened_species_number,
        ( sum (threatened_inc) :: REAL / count (*) ) * 100 AS threatened_percentage,
        'per'                                                AS um
      FROM
        ( SELECT
            *,
            CASE WHEN threatened
              THEN 1
            ELSE 0 END AS threatened_inc
          FROM
            answer_1_2 ) raw
      GROUP BY
        iteration
      UNION
      SELECT
        '3_2'                                                          AS indicator,
        iteration,
        'na'                                                           AS country_iso3,
        'WITC'                                                         AS wiews_region,
        500                                                            AS rank,

        sum (varieties)                                                AS varieties_number,
        sum (threatened_varieties)                                     AS threatened_varieties_number,
        ( sum (threatened_varieties) :: REAL / sum (varieties) ) * 100 AS threatened_percentage,
        'per'                                                            AS um
      FROM
        ( SELECT
            *,
            CASE WHEN threatened
              THEN 1
            ELSE 0 END AS threatened_inc
          FROM
            answer_1_2 ) raw
      GROUP BY
        iteration
      HAVING sum (varieties) > 0
      ORDER BY
        indicator,
        iteration

    ),



      total_region as (

      SELECT
        '3_1'                                              AS indicator,
        iteration,
        'na'                                               AS country_iso3,
        b.w                                                AS wiews_region,
        b.rank                                             AS rank,
        count (*)                                          AS species_number,
        sum (threatened_inc)                               AS threatened_species_number,
        ( sum (threatened_inc) :: REAL / count (*) ) * 100 AS threatened_percentage,
        'per'                                                AS um
      FROM
        ( SELECT
            *,
            CASE WHEN threatened
              THEN 1
            ELSE 0 END AS threatened_inc
          FROM
            answer_1_2 ) raw
        JOIN codelist.ref_region_country b on (raw.country_iso3 = b.country_iso3 )
      GROUP BY
        iteration,
        wiews_region,
        rank
      UNION
      SELECT
        '3_2'                                                          AS indicator,
        iteration,
        'na'                                                           AS country_iso3,
        b.w                                                            AS wiews_region,
        b.rank                                                         AS rank,
        sum (varieties)                                                AS varieties_number,
        sum (threatened_varieties)                                     AS threatened_varieties_number,
        ( sum (threatened_varieties) :: REAL / sum (varieties) ) * 100 AS threatened_percentage,
        'per'                                                            AS um
      FROM
        ( SELECT
            *,
            CASE WHEN threatened
              THEN 1
            ELSE 0 END AS threatened_inc
          FROM
            answer_1_2 ) raw
        JOIN codelist.ref_region_country b on (raw.country_iso3 = b.country_iso3 )

      GROUP BY
        iteration,
        wiews_region,
        rank
      HAVING sum (varieties) > 0
      ORDER BY
        indicator,
        iteration,
        wiews_region
    ),
      worlds as (

      SELECT
        '3_1'                                              AS indicator,
        iteration,
        'na'                                               AS country_iso3,
        '5000'                                             AS wiews_region,
        300                                                AS rank,
        count (*)                                          AS species_number,
        sum (threatened_inc)                               AS threatened_species_number,
        ( sum (threatened_inc) :: REAL / count (*) ) * 100 AS threatened_percentage,
        'per'                                                AS um
      FROM
        ( SELECT
            *,
            CASE WHEN threatened
              THEN 1
            ELSE 0 END AS threatened_inc
          FROM
            answer_1_2 ) raw
      GROUP BY
        iteration
      UNION
      SELECT
        '3_2'                                                          AS indicator,
        iteration,
        'na'                                                           AS country_iso3,
        '5000'                                                         AS wiews_region,
        300                                                            AS rank,
        sum (varieties)                                                AS varieties_number,
        sum (threatened_varieties)                                     AS threatened_varieties_number,
        ( sum (threatened_varieties) :: REAL / sum (varieties) ) * 100 AS threatened_percentage,
        'per'                                                            AS um
      FROM
        ( SELECT
            *,
            CASE WHEN threatened
              THEN 1
            ELSE 0 END AS threatened_inc
          FROM
            answer_1_2 ) raw
      GROUP BY
        iteration
      HAVING sum (varieties) > 0
      ORDER BY
        indicator,
        iteration),

      nfp as (SELECT
                '1130'                      AS domain,
                cast (indicator_id AS TEXT) AS indicator,
                iteration,
                'nfp'::text as element,

                cast (iso AS TEXT)          AS country,
                cast (iso AS TEXT)          AS wiews_region,
                1::INTEGER                  AS rank,
                0                           AS sum,
                0                           AS threatened_sum,
                nfp_rating                  AS percentage,
                'per'::text                         AS um
              FROM
                indicator_analysis spec
                JOIN
                ref_country
                  ON ( ref_country.country_id = spec.country_id )
              WHERE
                indicator_id = 3)
  /* --- START QUERY */



  /* Indicator */
  SELECT
    '1130' AS domain,
    indicator,
    iteration::text,
    'ind' as element,
    country_iso3,
    wiews_region,
    rank,
    species_number,
    threatened_species_number,
    threatened_percentage,
    um

  FROM
    total
  UNION

  /* WITC aggregation */
  SELECT
    '1130' AS domain,
    indicator,
    iteration::text,
    'ind' as element,
    country_iso3,
    wiews_region,
    rank,
    species_number,
    threatened_species_number,
    threatened_percentage,
    um

  FROM
    total_witc

  UNION

  /* Indicator Regional aggregation */
  SELECT
    '1130' AS domain,
    indicator,
    iteration::text,
    'ind' as element,
    country_iso3,
    wiews_region,
    rank,
    species_number,
    threatened_species_number,
    threatened_percentage,
    um
  FROM
    total_region


  UNION


  /* analysis */

  SELECT
    '1130'::text                AS domain,
    cast (indicator_id AS TEXT) AS indicator,
    iteration::text,
    'nfp'::text as element,

    cast (iso AS TEXT)          AS country,
    cast (iso AS TEXT)          AS wiews_region,
    1::INTEGER                  AS rank,
    0                           AS sum,
    0                           AS threatened_sum,
    nfp_rating                  AS percentage,
    'per'::text                   AS um
  FROM
    indicator_analysis spec
    JOIN
    ref_country
      ON ( ref_country.country_id = spec.country_id )
  WHERE
    indicator_id = 3

  UNION

  /* analysis regional*/
  SELECT
    domain,
    indicator,
    iteration::text,
    element,
    'na'::text                  as country_iso3,
    b.w                         as wiews_region,
    b.rank                      as rank,
    0                           AS sum,
    0                           AS threatened_sum,
    avg(percentage)             AS percentage,
    'per'::text                   AS um

  FROM
    (
      SELECT
        '1130'::text                      AS domain,
        cast (indicator_id AS TEXT) AS indicator,
        iteration,
        'nfp'::text as element,

        cast (iso AS TEXT)          AS country,
        cast (iso AS TEXT)          AS wiews_region,
        0                           AS sum,
        0                           AS threatened_sum,
        nfp_rating                  AS percentage,
        'per'::text                         AS um
      FROM
        indicator_analysis spec
        JOIN
        ref_country
          ON ( ref_country.country_id = spec.country_id )
      WHERE
        indicator_id = 3
    )a JOIN  codelist.ref_region_country b on (a.country = b.country_iso3 )
  GROUP BY domain, indicator, iteration,element, w, rank

);