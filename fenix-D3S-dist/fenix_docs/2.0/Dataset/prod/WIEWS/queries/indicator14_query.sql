DROP TABLE IF EXISTS indicators.indicator14;

CREATE TABLE indicators.indicator14 as (
  with raw as (
      SELECT
        ref_c.iso,
        iteration::TEXT,
        c.wiews_instcode as stakeholder,
        answer_freetext,
        answer_freetext_local,
        reference_id
      FROM
        answer a
        JOIN answer_detail ad ON ( a.id = answerid and ad.subquestionid= 1058)
        LEFT JOIN ref_instab c ON (a.orgid = c.id)
        LEFT JOIN ref_country ref_c ON (ref_c.country_id = a.country_id and ref_c.lang = 'EN')
      WHERE questionid = 11
  ),

      ind_14 as (

        SELECT
          iteration::TEXT,
          iso as country,
          stakeholder,
          lower(CASE
          WHEN reference_id is null or reference_id = '0'  and answer_freetext is not null THEN answer_freetext
          WHEN reference_id is null or reference_id = '0'  and answer_freetext is null and  answer_freetext_local is not null THEN  answer_freetext_local
          WHEN reference_id is not null then crop_name end) as crop,
          count(*) as value
        FROM
          raw a
          FULL JOIN ( SELECT * FROM ref_crop WHERE lang = 'EN')b on (a.reference_id = b.crop_id::TEXT )
        WHERE iteration is not null
        GROUP BY
          iteration,
          iso,
          stakeholder,
          crop),


      rating as (
        SELECT
          a.iteration::TEXT,
          ref_c.iso as country,
          a.nfp_rating as value
        FROM
          indicator_analysis a
          JOIN
          ref_country ref_c
            ON ref_c.country_id = a.country_id
        WHERE indicator_id = 14 and nfp_rating>0
    )

  SELECT
    *
  FROM
    ( /* by indicator */
      SELECT
        '2120'              AS domain,
        '14'                AS indicator,
        iteration,
        country,
        country::TEXT             AS wiews_region,
        1 :: INTEGER        AS rank,
        'ind'               AS element,
        cast ('na' AS TEXT) AS stakeholder,
        cast ('na' AS TEXT) AS crop,
        count (DISTINCT(crop))         AS value,
        'num'               AS um
      FROM
        ind_14
      GROUP BY
        iteration,
        country
      UNION
      /* by stakeholder */

      SELECT
        '2120'              AS domain,
        '14'                AS indicator,
        iteration,
        country,
        country::TEXT              AS wiews_region,
        1 :: INTEGER        AS rank,
        'stk'               AS element,
        stakeholder,
        cast ('na' AS TEXT) AS crop,
        count (DISTINCT(crop))         AS value,
        'num'               AS um
      FROM
        ind_14
      GROUP BY
        iteration,
        country,
        stakeholder
      UNION
      /* by crop */

      SELECT
        '2120'              AS domain,
        '14'                AS indicator,
        iteration,
        country,
        country::TEXT              AS wiews_region,
        1 :: INTEGER        AS rank,
        'crp'               AS element,
        stakeholder,
        crop AS crop,
        sum (value)         AS value,
        'num'               AS um
      FROM
        ind_14
      GROUP BY
        iteration,
        country,
        stakeholder,
        crop
      UNION

      /* rating */
      SELECT
        '2120'              AS domain,
        '14'                AS indicator,
        iteration,
        country,
        country::TEXT              AS wiews_region,
        1 :: INTEGER        AS rank,
        'nfp'               AS element,
        cast ('na' AS TEXT) AS stakeholder,
        cast ('na' AS TEXT) AS crop,
        value,
        'num'               AS um
      FROM
        rating )x

  ORDER BY country, element, stakeholder);

/* regional aggregation for indicator*/
INSERT INTO indicators.indicator14
  SELECT
    max(domain) as domain,
    max(indicator) as indicator,
    iteration,
    cast('na' as TEXT) as country,
    w as wiews_region,
    rank as rank,
    'ind' as element,
    cast('na' as TEXT) as stakeholder,
    cast('na' as TEXT) as crop,
    count(distinct(crop)) as value,
    'num'               AS um
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
            a.crop
          FROM
            codelist.ref_region_country b JOIN (SELECT * from indicators.indicator14 where element='crp')a
          ON a.country = b.country_iso3)z


GROUP BY iteration, wiews_region, rank;

/* regional aggregation for nfp*/
INSERT INTO indicators.indicator14
  SELECT
    max(a.domain) as domain,
    max(a.indicator) as indicator,
    a.iteration::TEXT,
    cast('na' as TEXT) as country,
    b.w as wiews_region,
    b.rank as rank,
    'nfpa'::TEXT as element,
    cast('na' as TEXT) as stakeholder,
    cast('na' as TEXT) as crop,
    avg(value) as value,
    max(um) as um
  from codelist.ref_region_country b
    JOIN (SELECT * from indicators.indicator14 WHERE element = 'nfp')a ON a.country = b.country_iso3
  GROUP BY
    a.iteration,
    b.w,
    b.rank
  ORDER BY
    iteration,
    rank;








