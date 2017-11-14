DROP TABLE IF EXISTS indicators.indicator15;

CREATE TABLE indicators.indicator15 AS (

  WITH
      itw AS (
      --SELECT version AS iteration, 365.0 / (end_date - start_date) AS year_weight FROM iteration
        SELECT version as iteration, 12.0 / (Date_part('year', end_date) * 12 + Date_part('month', end_date) - Date_part('year', start_date) * 12 + Date_part('month', start_date) - 1) AS year_weight FROM iteration
    ),
      crops AS (
        SELECT
          c.answerid,
          c.subquestionid,
          crop_id,
          coalesce(crop_name, answer_freetext) AS crop_name_en
        FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id AND lang = 'EN')
        WHERE c.subquestionid = 1068
    ),
      raw AS (
        SELECT
          a.*,
          co.iso                                                             AS country_iso,
          lower(f.answer_freetext)                                           AS answare,
          t_start_date.answer_freetext                                       AS start_date,
          coalesce(t_end_date.answer_freetext, t_start_date.answer_freetext) AS end_date,
          c.crop_name_en,
          it.WIEWS_INSTCODE                                                  AS stakeholder_code,
          it.orgname                                                         AS stakeholder_name
        FROM
          answer a
          LEFT JOIN answer_detail f ON (f.answerId = a.id AND f.subquestionId = 1065)
          LEFT JOIN answer_detail t_start_date ON (t_start_date.answerId = a.id AND t_start_date.subquestionId = 1066)
          LEFT JOIN answer_detail t_end_date ON (t_end_date.answerId = a.id AND t_end_date.subquestionId = 1067)
          LEFT JOIN crops c ON (c.answerId = a.id)
          LEFT JOIN ref_instab it ON (it.id = a.orgId)
          LEFT JOIN ref_country co ON (co.country_id = a.country_id)
        WHERE a.approved = 1 and questionid = 12
    ),

      stakeholder_raw AS (
        SELECT
          iteration,
          country_iso,
          stakeholder_code,
          answare
        FROM raw
        GROUP BY iteration, country_iso, stakeholder_code, answare
    ),
      country_raw AS (
        SELECT
          iteration,
          country_iso,
          answare
        FROM stakeholder_raw
        GROUP BY iteration, country_iso, answare
    ),
      region_raw AS (
        SELECT
          iteration,
          w AS wiews_region,
          answare,
          rank
        FROM country_raw i
          JOIN codelist.ref_region_country r ON (i.country_iso = r.country_iso3)
        GROUP BY iteration, w, rank, answare
    ),

      stakeholder AS (
        SELECT
          sr.iteration::text,
          country_iso,
          stakeholder_code,
          count(*) AS total,
          count(*)::REAL * year_weight AS average
        FROM stakeholder_raw sr
          JOIN itw ON (sr.iteration = itw.iteration)
        GROUP BY sr.iteration, itw.year_weight, country_iso, stakeholder_code
    ),
      country AS (
        SELECT
          cr.iteration::text,
          country_iso,
          count(*) AS total,
          count(*)::REAL * year_weight AS average
        FROM country_raw cr
          JOIN itw ON (cr.iteration = itw.iteration)
        GROUP BY cr.iteration, itw.year_weight, country_iso
    ),
      region AS (
        SELECT
          rr.iteration::text,
          wiews_region,
          count(*) AS total,
          count(*)::REAL * year_weight AS average,
          rank
        FROM region_raw rr
          JOIN itw ON (rr.iteration = itw.iteration)
        GROUP BY rr.iteration, itw.year_weight, wiews_region, rank
    ),

      nfp AS (
        SELECT
          c.iso AS country_iso,
          spec.iteration::TEXT,
          nfp_rating::REAL
        FROM indicator_analysis spec
          JOIN ref_country c ON (c.country_id = spec.country_id)
        WHERE indicator_id = 15 AND nfp_rating > 0
    ),
      nfp_region AS (
        SELECT
          w AS wiews_region,
          nfp.iteration::TEXT,
          avg(nfp_rating)::REAL AS nfp_rating,
          rank
        FROM nfp
          JOIN codelist.ref_region_country r ON (nfp.country_iso = r.country_iso3)
        GROUP BY nfp.iteration, w, r.rank
    )

  --stakeholder
  SELECT
    '2130'::TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    stakeholder_code AS stakeholder,
    '15'::TEXT     AS indicator,
    'stk_t'::TEXT  AS element,
    iteration,
    total            AS value,
    'num'::TEXT    AS um,
    1 AS rank
  FROM stakeholder
  UNION ALL
  SELECT
    '2130'::TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    stakeholder_code AS stakeholder,
    '15'::TEXT     AS indicator,
    'stk_a'::TEXT  AS element,
    iteration,
    average            AS value,
    'num'::TEXT    AS um,
    1 AS rank
  FROM stakeholder
  --country
  UNION ALL
  SELECT
    '2130' :: TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    'na'::TEXT AS stakeholder,
    '15'::TEXT     AS indicator,
    'ind_t'::TEXT  AS element,
    iteration::TEXT,
    total            AS value,
    'num'::TEXT    AS um,
    1 AS rank
  FROM country
  UNION ALL
  SELECT
    '2130' :: TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    'na'::TEXT AS stakeholder,
    '15'::TEXT     AS indicator,
    'ind_a'::TEXT  AS element,
    iteration::TEXT,
    average AS value,
    'num'::TEXT AS um,
    1 AS rank
  FROM country
  --region total
  UNION ALL
  SELECT
    '2130'::TEXT   AS domain,
    wiews_region,
    'na'::TEXT             AS country_iso3,
    'na'::TEXT AS stakeholder,
    '15'::TEXT     AS indicator,
    'ind_t'::TEXT  AS element,
    iteration::TEXT,
    total            AS value,
    'num'::TEXT    AS um,
    rank
  FROM region
  --region average
  UNION ALL
  SELECT
    '2130'::TEXT   AS domain,
    wiews_region,
    'na'::TEXT             AS country_iso3,
    'na'::TEXT AS stakeholder,
    '15'::TEXT     AS indicator,
    'ind_a'::TEXT  AS element,
    iteration::TEXT,
    average            AS value,
    'num'::TEXT    AS um,
    rank
  FROM region
  --National focal point rating
  UNION ALL
  SELECT
    '2130'::TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    'na'::TEXT AS stakeholder,
    '15'::TEXT     AS indicator,
    'nfp'::TEXT  AS element,
    iteration::TEXT,
    nfp_rating            AS value,
    'num'::TEXT    AS um,
    1 AS rank
  FROM nfp
  UNION ALL
  SELECT
    '2130'::TEXT   AS domain,
    wiews_region,
    'na'::TEXT AS country_iso3,
    'na'::TEXT AS stakeholder,
    '15'::TEXT     AS indicator,
    'nfpa'::TEXT  AS element,
    iteration::TEXT,
    nfp_rating            AS value,
    'num'::TEXT    AS um,
    rank
  FROM nfp_region

);