DROP TABLE IF EXISTS indicators.indicator15;

CREATE TABLE indicators.indicator15 AS (

  WITH
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
          f.answer_freetext                                                  AS answare,
          t_start_date.answer_freetext                                       AS start_date,
          coalesce(t_end_date.answer_freetext, t_start_date.answer_freetext) AS end_date,
          c.crop_name_en,
          it.WIEWS_INSTCODE                                                  AS stakeholder_code,
          it.orgname                                                         AS stakeholder_name
        FROM
          (SELECT *
           FROM answer
           WHERE questionid = 12) a
          LEFT JOIN answer_detail f ON (f.answerId = a.id AND f.subquestionId = 1065)
          LEFT JOIN answer_detail t_start_date ON (t_start_date.answerId = a.id AND t_start_date.subquestionId = 1066)
          LEFT JOIN answer_detail t_end_date ON (t_end_date.answerId = a.id AND t_end_date.subquestionId = 1067)
          LEFT JOIN crops c ON (c.answerId = a.id)
          LEFT JOIN ref_instab it ON (it.id = a.orgId)
          LEFT JOIN ref_country co ON (co.country_id = a.country_id)
        WHERE a.approved = 1
    ),
      indicator_raw AS (
        SELECT
          iteration,
          country_iso,
          stakeholder_code,
          CASE WHEN start_date NOTNULL AND char_length(start_date) >= 4
            THEN substring(start_date FROM 1 FOR 4) :: INTEGER
          ELSE NULL END                            AS start_date,
          CASE WHEN end_date NOTNULL AND char_length(end_date) >= 4
            THEN substring(end_date FROM 1 FOR 4) :: INTEGER
          ELSE NULL END                            AS end_date,
          lower(answare || start_date || end_date) AS answare
        FROM raw
    ),
      stakeholder_raw AS (
        SELECT
          iteration,
          country_iso,
          stakeholder_code,
          min(start_date) AS start_date,
          max(end_date)   AS end_date,
          answare
        FROM indicator_raw
        --cleaning data about start date and end date values
        WHERE start_date IS NOT NULL AND end_date IS NOT NULL
        GROUP BY iteration, country_iso, stakeholder_code, answare
        HAVING min(start_date) <= max(end_date)
    ),
      country_raw AS (
        SELECT
          iteration,
          country_iso,
          'na' :: TEXT    AS stakeholder_code,
          min(start_date) AS start_date,
          max(end_date)   AS end_date,
          answare
        FROM indicator_raw
        --cleaning data about start date and end date values
        WHERE start_date IS NOT NULL AND end_date IS NOT NULL
        GROUP BY iteration, country_iso, stakeholder_code, answare
        HAVING min(start_date) <= max(end_date)
    ),
      region_raw AS (
        SELECT
          iteration,
          w               AS wiews_region,
          'na' :: TEXT    AS stakeholder_code,
          min(start_date) AS start_date,
          max(end_date)   AS end_date,
          answare
        FROM indicator_raw i
          JOIN codelist.ref_region_country r ON (i.country_iso = r.country_iso3)
        --cleaning data about start date and end date values
        WHERE start_date IS NOT NULL AND end_date IS NOT NULL
        GROUP BY iteration, w, answare
        HAVING min(start_date) <= max(end_date)
    ),
      stakeholder_total AS (
        SELECT
          iteration,
          country_iso,
          stakeholder_code,
          min(start_date) AS start_date,
          max(end_date)   AS end_date,
          count(*)        AS total
        FROM stakeholder_raw
        GROUP BY iteration, country_iso, stakeholder_code
    ),
      country_total AS (
        SELECT
          iteration,
          country_iso,
          stakeholder_code,
          min(start_date) AS start_date,
          max(end_date)   AS end_date,
          count(*)        AS total
        FROM country_raw
        GROUP BY iteration, country_iso, stakeholder_code
    ),
      region_total AS (
        SELECT
          iteration,
          wiews_region,
          stakeholder_code,
          min(start_date) AS start_date,
          max(end_date)   AS end_date,
          count(*)        AS total
        FROM region_raw
        GROUP BY iteration, wiews_region, stakeholder_code
    ),
      nfp AS (
        SELECT
          '2130' :: TEXT     AS domain,
          c.iso              AS wiews_region,
          c.iso              AS country_iso3,
          'na' :: TEXT       AS stakeholder,
          '15' :: TEXT       AS indicator,
          'nfp' :: TEXT      AS element,
          spec.iteration :: TEXT,
          start_date         AS startdate,
          end_date           AS enddate,
          nfp_rating :: REAL AS value,
          'num' :: TEXT      AS um,
          1 :: INTEGER       AS rank
        FROM indicator_analysis spec
          JOIN ref_country c ON (c.country_id = spec.country_id)
          JOIN country_total ct ON (c.iso = ct.country_iso)
        WHERE indicator_id = 15
    )

  --stakeholder total
  SELECT
    '2130' :: TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    stakeholder_code AS stakeholder,
    '15' :: TEXT     AS indicator,
    'stk_t' :: TEXT  AS element,
    iteration :: TEXT,
    start_date       AS startdate,
    end_date         AS enddate,
    total            AS value,
    'num' :: TEXT    AS um,
    1                AS rank
  FROM stakeholder_total t
  --stakeholder average
  UNION ALL
  SELECT
    '2130' :: TEXT                      AS domain,
    country_iso                         AS wiews_region,
    country_iso                         AS country_iso3,
    stakeholder_code                    AS stakeholder,
    '15' :: TEXT                        AS indicator,
    'stk_a' :: TEXT                     AS element,
    iteration :: TEXT,
    start_date                          AS startdate,
    end_date                            AS enddate,
    total / (end_date - start_date + 1) AS value,
    'num' :: TEXT                       AS um,
    1                                   AS rank
  FROM stakeholder_total t
  --country total
  UNION ALL
  SELECT
    '2130' :: TEXT   AS domain,
    country_iso      AS wiews_region,
    country_iso      AS country_iso3,
    stakeholder_code AS stakeholder,
    '15' :: TEXT     AS indicator,
    'ind_t' :: TEXT  AS element,
    iteration :: TEXT,
    start_date       AS startdate,
    end_date         AS enddate,
    total            AS value,
    'num' :: TEXT    AS um,
    1                AS rank
  FROM country_total t
  --country average
  UNION ALL
  SELECT
    '2130' :: TEXT                      AS domain,
    country_iso                         AS wiews_region,
    country_iso                         AS country_iso3,
    stakeholder_code                    AS stakeholder,
    '15' :: TEXT                        AS indicator,
    'ind_a' :: TEXT                     AS element,
    iteration :: TEXT,
    start_date                          AS startdate,
    end_date                            AS enddate,
    total / (end_date - start_date + 1) AS value,
    'num' :: TEXT                       AS um,
    1                                   AS rank
  FROM country_total t
  --region total
  UNION ALL
  SELECT
    '2130' :: TEXT   AS domain,
    wiews_region,
    'na'             AS country_iso3,
    stakeholder_code AS stakeholder,
    '15' :: TEXT     AS indicator,
    'ind_t' :: TEXT  AS element,
    iteration :: TEXT,
    start_date       AS startdate,
    end_date         AS enddate,
    total            AS value,
    'num' :: TEXT    AS um,
    1                AS rank
  FROM region_total t
  --region average
  UNION ALL
  SELECT
    '2130' :: TEXT                      AS domain,
    wiews_region,
    'na'                                AS country_iso3,
    stakeholder_code                    AS stakeholder,
    '15' :: TEXT                        AS indicator,
    'ind_a' :: TEXT                     AS element,
    iteration :: TEXT,
    start_date                          AS startdate,
    end_date                            AS enddate,
    total / (end_date - start_date + 1) AS value,
    'num' :: TEXT                       AS um,
    1                                   AS rank
  FROM region_total t
  --National focal point rating
  UNION ALL
  SELECT *
  FROM nfp
  --Regional focal point rating average
  UNION ALL
  SELECT
    domain,
    w as wiews_region,
    'na'           AS country_iso3,
    stakeholder,
    indicator,
    'nfpa' :: TEXT    element,
    iteration,
    min(startdate) AS startdate,
    max(enddate)   AS enddate,
    avg(value)     AS value,
    'num'          AS um,
    r.rank
  FROM nfp
    JOIN codelist.ref_region_country r ON (nfp.country_iso3 = r.country_iso3)
  GROUP BY domain, w, indicator, stakeholder, iteration, r.rank

);