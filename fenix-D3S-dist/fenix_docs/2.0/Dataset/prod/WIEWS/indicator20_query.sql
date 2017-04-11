
with rating as (   SELECT
                     cast(iteration as TEXT) as iteration,
                     cast('2240' as TEXT) as domain,
                     cast('nfp' as TEXT) as element,
                     cast('20' as TEXT) as indicator,
                     cast(null as TEXT) as biologicalAccessionId,
                     country,
                     country as m49_country,
                     cast('ZZZ' as TEXT) as stakeholder,
                     cast('na' as TEXT) as genus,
                     nfp_rating as VALUE,
                     cast('%' as TEXT) as um
                     from (
                        SELECT
                          b.ISO as country,
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
                     country
                ),

    country as (SELECT
                    iteration,
                    domain,
                    element,
                    indicator,
                    cast(null as TEXT) as biologicalAccessionId,

                    country,
                    m49_country,
                    stakeholder,
                    genus,
                    sum(value) as VALUE,
                    um
                    FROM (
                      SELECT
                        cast(a.iteration as text) as iteration,
                        cast('2240' as TEXT) as domain,
                        cast('ind'  as TEXT) as element,
                        cast('20'  as TEXT)  as indicator,
                        b.ISO as country,
                        b.iso as m49_country,
                        cast('ZZZ' as TEXT)  as stakeholder,
                        cast('na'  as TEXT)  as genus,
                        count(distinct(CASE WHEN a.taxonid>0 THEN a.taxonid::TEXT ELSE a.taxon_freetext end  )) as VALUE ,
                        cast('1'  as TEXT)   as um
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
                        stakeholder,
                        country ) z
                  GROUP BY
                  domain,
                  element,
                  indicator,
                  iteration,
                  stakeholder,
                  country,
                  m49_country,
                  genus,
                    um
                ),

    genus as ( SELECT
                  cast(ITERATION as TEXT) as iteration,
                  cast('2240' as TEXT) as domain,
                  cast('gen' as TEXT) as element,
                  cast('20' as TEXT) as indicator,
                  cast(a.biologicalAccessionId as TEXT) as biologicalAccessionId,
                  b.ISO as country,
                  b.ISO as  m49_country,
                  d.WIEWS_INSTCODE as stakeholder,
                  cast(lower(COALESCE(c.GENUS,'NA')) as TEXT) as genus,
                  count(distinct(CASE WHEN a.taxonid>0 THEN a.taxonid::TEXT ELSE a.taxon_freetext end  )) as VALUE ,
                  cast('1'as TEXT) as um

                  FROM answer_q14 a

                  JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID
                  JOIN ref_taxtab c on a.taxonId = c.TAXON_ID
                  JOIN ref_instab d on d.ID = a.orgId

                  WHERE a.approved = 1

                  GROUP BY
                  iteration,
                  biologicalAccessionId,
                  stakeholder,
                  country,
                  genus
              ),

    classic as (SELECT

                  cast(ITERATION as TEXT) as iteration,
                  cast('2240'as TEXT) as domain,
                  cast('stk'as TEXT) as element,
                  cast('20' as TEXT) as indicator,
                  cast(a.biologicalAccessionId as text) as biologicalAccessionId,
                  b.ISO as country,
                  b.ISO as m49_country,
                  c.WIEWS_INSTCODE as stakeholder,
                  cast('na' as TEXT) as genus,
                  count(distinct(CASE WHEN a.taxonid>0 THEN a.taxonid::TEXT ELSE a.taxon_freetext end  )) as VALUE ,
                  cast('1' as TEXT) as um

                  FROM answer_q14 a

                  JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID
                  JOIN ref_instab c on a.orgId = c.ID

                  WHERE a.approved = 1

                  GROUP BY
                  iteration,
                  stakeholder,
                  biologicalAccessionId,
                  country
                )


    select * from classic

    UNION
    /* By rating*/
    select * from rating

    union
    /* By country*/
    select * from country

    UNION
    /* By genus*/
    select * from genus




