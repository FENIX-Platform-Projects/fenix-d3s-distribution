WITH total_donor_oda as (
      SELECT year,
      sum(value) as value,
        'TOTAL DONOR'::text as indicator,
      'Million usda'::text as um

      FROM data.adam_browse_sector_oda
      WHERE donorcode='1'
            and fao_sector='1'
            and oda = 'usd_commitment'
            and year >=2011
      GROUP BY year
),
  gni_donor_oda as (
      SELECT year,
        sum(value) as value,
        'Resource partner GNI'::text as indicator,
        'Million usda'::text as um
      FROM data.adam_donors_gni
      WHERE donorcode='1'
            and year >=2011 and YEAR <=2015
      GROUP BY year
  ),

oecd_oda_gni as (

    SELECT
      all_subs_sum.year,
      (all_subs_sum.value/gni_sum.value)*100 as value,
    '% OECD Average of ODA/GNI'::text as indicator,
      '%'::text  as um
    from (
  SELECT year,
    sum(value) as value
  FROM data.adam_browse_sector_oda
  WHERE dac_member='t'
        and donorcode='NA'
        and purposecode = 'NA'
        and oda = 'usd_commitment'
        and year >=2011
  GROUP BY year)all_subs_sum JOIN (
      SELECT year,
        sum(value) as value,
        'Million usda' as um
      FROM data.adam_donors_gni
      WHERE year >=2011 and YEAR <=2015
      GROUP BY year)gni_sum on all_subs_sum.year = gni_sum.year
),

 oda_on_gni as (
      SELECT
        total_donor_oda.year,
        (total_donor_oda.value / gni_donor_oda.value)*100 as value ,
        '% ODA/GNI'::text as indicator,
        '%'::text as um
      from
        total_donor_oda JOIN gni_donor_oda ON total_donor_oda.year = gni_donor_oda.year
  )

SELECT * from total_donor_oda
UNION
  SELECT * from gni_donor_oda
UNION
  SELECT * from oda_on_gni
UNION
  SELECT * from oecd_oda_gni


