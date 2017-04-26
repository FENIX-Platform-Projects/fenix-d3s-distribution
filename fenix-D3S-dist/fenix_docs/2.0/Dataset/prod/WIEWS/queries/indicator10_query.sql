DROP TABLE IF EXISTS indicators.indicator10;

CREATE TABLE indicators.indicator10 as (

  WITH raw AS (
      SELECT
        c.iso,
        a.*,
        coalesce(t1.answer_freetext, '0') :: REAL AS sites_with_management,
        coalesce(t2.answer_freetext, '0') :: REAL    sites_total
      FROM answer a
        JOIN answer_detail t1 ON (t1.answerId = a.id)
        JOIN answer_detail t2 ON (t2.answerId = a.id)
        JOIN ref_country c ON (c.country_id = a.country_id)
      WHERE a.approved = 1 AND t1.subquestionid = 1043 AND t2.subquestionid = 1042
  ),
      indicator AS (
        SELECT
          '1410' :: TEXT                                                 AS domain,
          iso                                                            AS wiews_region,
          '10' :: TEXT                                                   AS indicator,
          'ind' :: TEXT                                                  AS element,
          iteration,
          CASE WHEN sum(sites_total) = 0
            THEN 0
          ELSE (sum(sites_with_management) / sum(sites_total)) * 100 END AS value,
          'per' :: TEXT                                                  AS um,
          sum(sites_with_management)                                     AS sites_with_management,
          sum(sites_total)                                               AS sites_total,
          iso                                                            AS country_iso3,
          1 :: INTEGER                                                   AS rank
        FROM raw
        GROUP BY iteration, iso
    ),
      nfp AS (
        SELECT
          '1410' :: TEXT     AS domain,
          c.iso              AS wiews_region,
          '10' :: TEXT       AS indicator,
          'nfp' :: TEXT      AS element,
          iteration,
          nfp_rating :: REAL AS value,
          'per' :: TEXT      AS um,
          0 :: REAL          AS sites_with_management,
          0 :: REAL          AS sites_total,
          c.iso              AS country_iso3,
          1 :: INTEGER       AS rank
        FROM indicator_analysis spec
          JOIN ref_country c ON (c.country_id = spec.country_id)
        WHERE indicator_id = 10
    )
  --Indicator data
  SELECT *
  FROM indicator
  --National focal point rating
  UNION
  SELECT *
  FROM nfp

  --Regional data
  UNION
  SELECT
    domain,
    w,
    indicator,
    element,
    iteration,
    CASE WHEN sum(sites_total) = 0
      THEN 0
    ELSE (sum(sites_with_management) / sum(sites_total)) * 100 END AS value,
    'per'                                                          AS um,
    sum(sites_with_management)                                     AS sites_with_management,
    sum(sites_total)                                               AS sites_total,
    'na'                                                           AS country_iso3,
    r.rank
  FROM indicator i
    JOIN codelist.ref_region_country r ON (i.country_iso3 = r.country_iso3)
  GROUP BY domain, w, indicator, element, iteration, r.rank
  --Regional focal point rating average
  UNION
  SELECT
    domain,
    w,
    indicator,
    'nfpa' :: TEXT element,
    iteration,
    avg(value) AS  value,
    'per'      AS  um,
    0 :: REAL  AS  sites_with_management,
    0 :: REAL  AS  sites_total,
    'na'       AS  country_iso3,
    r.rank
  FROM nfp
    JOIN codelist.ref_region_country r ON (nfp.country_iso3 = r.country_iso3)
  GROUP BY domain, w, indicator, element, iteration, r.rank

  --Default ordering
  ORDER BY iteration, rank, wiews_region, element
);
