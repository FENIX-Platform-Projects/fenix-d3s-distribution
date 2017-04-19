DROP TABLE IF EXISTS indicators.indicator20;

CREATE TABLE indicators.indicator20 as (
  with rating as (   SELECT
                       cast(iteration as TEXT) as iteration,
                       cast('2240' as TEXT) as domain,
                       cast('nfp' as TEXT) as element,
                       cast('20' as TEXT) as indicator,
                       cast('na' as TEXT) as biologicalAccessionId,
                       country,
                       country as wiews_region,
                       rank,
                       cast('na' as TEXT) as stakeholder,
                       cast('na' as TEXT) as genus,
                       nfp_rating as VALUE,
                       cast('per' as TEXT) as um
                     from (
                            SELECT
                              b.ISO as country,
                              cast(1 as INTEGER)  as rank,
                              a.applicable,
                              a.data_available,
                              a.id,
                              a.nfp_rating,
                              a.iteration as iteration,
                              a.comment

                            FROM indicator_analysis a
                              JOIN ref_country b ON (a.country_id = b.COUNTRY_ID)
                            WHERE indicator_id = 20) t
                     GROUP BY
                       iteration,
                       domain,
                       element,
                       nfp_rating,
                       country,
                       rank
  ),

      country as (SELECT
                    iteration,
                    max(domain) as domain,
                    max(element) as element,
                    max(indicator) as indicator,
                    cast('na' as TEXT) as biologicalAccessionId,
                    country,
                    wiews_region,
                    rank,
                    max(stakeholder) as stakeholder,
                    max(genus) as genus,
                    sum(value) as VALUE,
                    max(um) as um
                  FROM (
                         SELECT
                           cast(a.iteration as text) as iteration,
                           cast('2240' as TEXT) as domain,
                           cast('ind'  as TEXT) as element,
                           cast('20'  as TEXT)  as indicator,
                           b.ISO as country,
                           b.iso as wiews_region,
                           cast(1 as INTEGER) as rank,
                           cast('na' as TEXT)  as stakeholder,
                           cast('na'  as TEXT)  as genus,
                           count(*) as VALUE ,
                           cast('num'  as TEXT)   as um
                         FROM
                           answer_q14 a
                           JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID
                           JOIN ref_instab c on a.orgId = c.ID

                         WHERE
                           a.approved  = 1
                           AND c.WIEWS_INSTCODE not in (
                             'BEL084',
                             'CIV033',
                             'COL003',
                             'ETH013',
                             'IND002',
                             'KEN056',
                             'MEX002',
                             'NGA039',
                             'PER001',
                             'PHL001',
                             'SYR002',
                             'TWN001',
                             'FJI049',
                             'SWE054')
                         GROUP BY
                           iteration,
                           country,
                           rank
                       ) z
                  GROUP BY
                    iteration,
                    country,
                    wiews_region,
                    rank
    ),

      worlds as ( SELECT
                    cast(a.iteration as text) as iteration,
                    cast('2240' as TEXT) as domain,
                    cast('ind'  as TEXT) as element,
                    cast('20'  as TEXT)  as indicator,
                    cast('na' as TEXT) as biologicalAccessionId,
                    cast('na'  as TEXT) as country,
                    cast('WITC'  as TEXT) as wiews_region,
                    cast(500  as INTEGER) as rank,
                    cast('na' as TEXT)  as stakeholder,
                    cast('na'  as TEXT)  as genus,
                    count(*) as VALUE ,
                    cast('num'  as TEXT)   as um
                  FROM
                    answer_q14 a

                  WHERE
                    a.approved  = 1

                  GROUP BY
                    iteration

                  UNION

                  SELECT
                    cast(a.iteration as text) as iteration,
                    cast('2240' as TEXT) as domain,
                    cast('ind'  as TEXT) as element,
                    cast('20'  as TEXT)  as indicator,
                    cast('na' as TEXT) as biologicalAccessionId,
                    cast('na'  as TEXT) as country,
                    cast('1'  as TEXT) as wiews_region,
                    cast(297  as INTEGER) as rank,
                    cast('na' as TEXT)  as stakeholder,
                    cast('na'  as TEXT)  as genus,
                    count(*) as VALUE ,
                    cast('num'  as TEXT)   as um
                  FROM
                    answer_q14 a
                    JOIN ref_instab c on a.orgId = c.ID

                  WHERE
                    a.approved  = 1  AND c.WIEWS_INSTCODE not in (
                      'BEL084',
                      'CIV033',
                      'COL003',
                      'ETH013',
                      'IND002',
                      'KEN056',
                      'MEX002',
                      'NGA039',
                      'PER001',
                      'PHL001',
                      'SYR002',
                      'TWN001',
                      'FJI049',
                      'SWE054')

                  GROUP BY
                    iteration
    ),


      genus as ( SELECT
                   cast(ITERATION as TEXT) as iteration,
                   cast('2240' as TEXT) as domain,
                   cast('gen' as TEXT) as element,
                   cast('20' as TEXT) as indicator,
                   cast(a.biologicalAccessionId as TEXT) as biologicalAccessionId,
                   b.ISO as country,
                   b.ISO as  wiews_region,
                   cast(1 as INTEGER) as rank,
                   d.WIEWS_INSTCODE as stakeholder,
                   cast(lower(COALESCE(c.GENUS,'NA')) as TEXT) as genus,
                   count(distinct(CASE WHEN a.taxonid>0 THEN a.taxonid::TEXT ELSE a.taxon_freetext end  )) as VALUE ,
                   cast('num'as TEXT) as um

                 FROM answer_q14 a

                   JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID
                   JOIN ref_taxtab c on a.taxonId = c.TAXON_ID
                   JOIN ref_instab d on d.ID = a.orgId

                 WHERE a.approved = 1
                       AND d.WIEWS_INSTCODE not in (
                   'BEL084',
                   'CIV033',
                   'COL003',
                   'ETH013',
                   'IND002',
                   'KEN056',
                   'MEX002',
                   'NGA039',
                   'PER001',
                   'PHL001',
                   'SYR002',
                   'TWN001',
                   'FJI049',
                   'SWE054')

                 GROUP BY
                   iteration,
                   biologicalAccessionId,
                   stakeholder,
                   country,
                   rank,
                   genus
    ),

      stakeholders as (SELECT

                         cast(ITERATION as TEXT) as iteration,
                         cast('2240'as TEXT) as domain,
                         cast('stk'as TEXT) as element,
                         cast('20' as TEXT) as indicator,
                         cast('na' as TEXT) as biologicalAccessionId,
                         b.ISO as country,
                         b.ISO as wiews_region,
                         cast(1 as INTEGER) as rank,
                         c.WIEWS_INSTCODE as stakeholder,
                         cast('na' as TEXT) as genus,
                         count(distinct(CASE WHEN a.taxonid>0 THEN a.taxonid::TEXT ELSE a.taxon_freetext end  )) as VALUE ,
                         cast('num' as TEXT) as um

                       FROM answer_q14 a

                         JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID
                         JOIN ref_instab c on a.orgId = c.ID

                       WHERE a.approved = 1
                             AND c.WIEWS_INSTCODE not in (
                         'BEL084',
                         'CIV033',
                         'COL003',
                         'ETH013',
                         'IND002',
                         'KEN056',
                         'MEX002',
                         'NGA039',
                         'PER001',
                         'PHL001',
                         'SYR002',
                         'TWN001',
                         'FJI049',
                         'SWE054')

                       GROUP BY
                         iteration,
                         stakeholder,
                         country,
                         rank
    )


  select * from stakeholders

  UNION
  /* By rating*/
  select * from rating

  union
  /* By country*/
  select * from country

  UNION
  /* By genus*/
  select * from genus

  UNION

  select * from worlds
);


/* regional aggregation for indicator*/
INSERT INTO indicators.indicator20
  SELECT
    a.iteration,
    max(a.domain) as domain,
    max(a.element) as element,
    max(a.indicator) as indicator,
    max(a.biologicalAccessionId) as biologicalAccessionId,
    cast('na' as TEXT) as country,
    b.w as wiews_region,
    b.rank as rank,
    max(a.stakeholder) as stakeholder,
    max(a.genus) as genus,
    sum(value) as value,
    max(um) as um



  from codelist.ref_region_country b JOIN (SELECT * from indicators.indicator20 WHERE element = 'ind')a ON a.country = b.country_iso3
  GROUP BY a.iteration,
    b.w,
    b.rank
  ORDER BY
    iteration,
    rank;

/* regional aggregation for nfp*/
INSERT INTO indicators.indicator20
  SELECT
    a.iteration,
    max(a.domain) as domain,
    max(a.element) as element,
    max(a.indicator) as indicator,
    max(a.biologicalAccessionId) as biologicalAccessionId,
    cast('na' as TEXT) as country,
    b.w as wiews_region,
    b.rank as rank,
    max(a.stakeholder) as stakeholder,
    max(a.genus) as genus,
    avg(value) as value,
    max(um) as um
  from codelist.ref_region_country b
    JOIN (SELECT * from indicators.indicator20 WHERE element = 'nfp')a ON a.country = b.country_iso3
  GROUP BY
    a.iteration,
    b.w,
    b.rank
  ORDER BY
    iteration,
    rank;







