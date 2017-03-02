
CREATE TABLE usd_aggregated_table2  as (

with disbursement as (

  SELECT
  oda ,
  channelsubcategory_code ,
  year ,
  value  ,
  parentsector_code ,
  purposecode ,
  recipientcode ,
  dac_member ,
  donorcode ,
  gaul0 ,
  unitcode ,
  fao_sector ,
  flowcategory ,
  fao_region

  from (
		select
			cast('usd_disbursement' as text) as oda,
			channelsubcategory_code,
			year,
			sum(value) as value,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode,
			  CASE WHEN purposecode in (
			      '12240',
			      '14030',
			      '14031',
			      '15170',
			      '16062',
			      '23070',
			      '31110',
			      '31120',
			      '31130',
			      '31140',
			      '31150',
			      '31161',
			      '31162',
			      '31163',
			      '31164',
			      '31165',
			      '31166',
			      '31181',
			      '31182',
			      '31191',
			      '31192',
			      '31193',
			      '31194',
			      '31195',
			      '31210',
			      '31220',
			      '31261',
			      '31281',
			      '31282',
			      '31291',
			      '31310',
			      '31320',
			      '31381',
			      '31382',
			      '31391',
			      '32161',
			      '32162',
			      '32163',
			      '32165',
			      '32267',
			      '41010',
			      '41020',
			      '41030',
			      '41040',
			      '41050',
			      '41081',
			      '41082',
			      '43040',
			      '43050',
			      '52010',
			      '72040',
            '74010') THEN '1' ELSE '0' END as fao_sector,
                               cast('10' as text) as flowcategory


			from usd_disbursement group by
			oda,
			year,
			channelsubcategory_code,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode)t
      LEFT JOIN
      faoregion
      ON t.recipientcode = faoregion.country
),


disbursement_defl as (
  SELECT
  oda ,
  channelsubcategory_code ,
  year ,
  value  ,
  parentsector_code ,
  purposecode ,
  recipientcode ,
  dac_member ,
  donorcode ,
  gaul0 ,
  unitcode ,
  fao_sector ,
  flowcategory ,
  fao_region
   from(
		select
			cast('usd_disbursement_defl' as text) as oda,
			channelsubcategory_code,
			year,
			sum(value) as value,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode,
			  CASE WHEN purposecode in (
			      '12240',
			      '14030',
			      '14031',
			      '15170',
			      '16062',
			      '23070',
			      '31110',
			      '31120',
			      '31130',
			      '31140',
			      '31150',
			      '31161',
			      '31162',
			      '31163',
			      '31164',
			      '31165',
			      '31166',
			      '31181',
			      '31182',
			      '31191',
			      '31192',
			      '31193',
			      '31194',
			      '31195',
			      '31210',
			      '31220',
			      '31261',
			      '31281',
			      '31282',
			      '31291',
			      '31310',
			      '31320',
			      '31381',
			      '31382',
			      '31391',
			      '32161',
			      '32162',
			      '32163',
			      '32165',
			      '32267',
			      '41010',
			      '41020',
			      '41030',
			      '41040',
			      '41050',
			      '41081',
			      '41082',
			      '43040',
			      '43050',
			      '52010',
			      '72040',
                               '74010') THEN '1' ELSE '0' END as fao_sector,
                                 cast('10' as text) as flowcategory


			from usd_disbursement_defl group by
			oda,
			year,
			channelsubcategory_code,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode)t
      LEFT JOIN
      faoregion
      ON t.recipientcode = faoregion.country
),

commitment as (

  SELECT
  oda ,
  channelsubcategory_code ,
  year ,
  value  ,
  parentsector_code ,
  purposecode ,
  recipientcode ,
  dac_member ,
  donorcode ,
  gaul0 ,
  unitcode ,
  fao_sector ,
  flowcategory ,
  fao_region
  from(
		select
			cast('usd_commitment' as text) as oda,
			channelsubcategory_code,
			year,
			sum(value) as value,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode,
			  CASE WHEN purposecode in (
			      '12240',
			      '14030',
			      '14031',
			      '15170',
			      '16062',
			      '23070',
			      '31110',
			      '31120',
			      '31130',
			      '31140',
			      '31150',
			      '31161',
			      '31162',
			      '31163',
			      '31164',
			      '31165',
			      '31166',
			      '31181',
			      '31182',
			      '31191',
			      '31192',
			      '31193',
			      '31194',
			      '31195',
			      '31210',
			      '31220',
			      '31261',
			      '31281',
			      '31282',
			      '31291',
			      '31310',
			      '31320',
			      '31381',
			      '31382',
			      '31391',
			      '32161',
			      '32162',
			      '32163',
			      '32165',
			      '32267',
			      '41010',
			      '41020',
			      '41030',
			      '41040',
			      '41050',
			      '41081',
			      '41082',
			      '43040',
			      '43050',
			      '52010',
			      '72040',
                               '74010') THEN '1' ELSE '0' END as fao_sector,
                                 cast('10' as text) as flowcategory


			from usd_commitment group by
			oda,
			year,
			channelsubcategory_code,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode)t
      LEFT JOIN
      faoregion
      ON t.recipientcode = faoregion.country
),

commitment_defl as (

  SELECT
  oda ,
  channelsubcategory_code ,
  year ,
  value  ,
  parentsector_code ,
  purposecode ,
  recipientcode ,
  dac_member ,
  donorcode ,
  gaul0 ,
  unitcode ,
  fao_sector ,
  flowcategory ,
  fao_region



  from(
		select
			cast('usd_commitment_defl' as text) as oda,
			channelsubcategory_code,
			year,
			sum(value) as value,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode,
			  CASE WHEN purposecode in (
			      '12240',
			      '14030',
			      '14031',
			      '15170',
			      '16062',
			      '23070',
			      '31110',
			      '31120',
			      '31130',
			      '31140',
			      '31150',
			      '31161',
			      '31162',
			      '31163',
			      '31164',
			      '31165',
			      '31166',
			      '31181',
			      '31182',
			      '31191',
			      '31192',
			      '31193',
			      '31194',
			      '31195',
			      '31210',
			      '31220',
			      '31261',
			      '31281',
			      '31282',
			      '31291',
			      '31310',
			      '31320',
			      '31381',
			      '31382',
			      '31391',
			      '32161',
			      '32162',
			      '32163',
			      '32165',
			      '32267',
			      '41010',
			      '41020',
			      '41030',
			      '41040',
			      '41050',
			      '41081',
			      '41082',
			      '43040',
			      '43050',
			      '52010',
			      '72040',
                               '74010') THEN '1' ELSE '0' END as fao_sector,
                                 cast('10' as text) as flowcategory


			from usd_commitment_defl group by
			oda,
			year,
			channelsubcategory_code,
			parentsector_code,
			purposecode,
			recipientcode,
			dac_member,
			donorcode,
			gaul0,
			unitcode)t
      LEFT JOIN
      faoregion
      ON t.recipientcode = faoregion.country
)

-- union each dataset aggregated
SELECT * from
  (

  SELECT * from (
    SELECT * from commitment
    UNION
    SELECT * from commitment_defl
    UNION
    SELECT * from disbursement
    UNION
    SELECT * from disbursement_defl
  )simple


  UNION

-- TOTAL with all NA
  SELECT * from
  (
    SELECT
    oda,
    'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    commitment
    GROUP BY
    oda,
    year

    UNION

    SELECT
    oda,
    'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from

    commitment_defl
    GROUP BY oda, year

     UNION

    SELECT
    oda,
     'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    disbursement
    GROUP BY oda, year

    UNION

    SELECT
    oda,
  'NA',
    year,

    sum(value) as value,
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    disbursement_defl
    GROUP BY oda, year
)tot



    UNION

-- TOTAL WITH DAC_MEMBER
  SELECT * from (
   SELECT
    oda,
    'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    MAX (dac_member) as dac_member,
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    commitment
    WHERE dac_member = 't'

    GROUP BY oda, year

    UNION

    SELECT
    oda,
    'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    MAX (dac_member) as dac_member,
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    commitment_defl
    WHERE dac_member = 't'

    GROUP BY oda, year

    UNION

    SELECT
   oda,
    'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    MAX (dac_member) as dac_member,
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    disbursement
    WHERE dac_member = 't'

    GROUP BY oda, year

    UNION

    SELECT
    oda,
    'NA',
    year,
    sum(value) as value,
    'NA',
    'NA',
    'NA',
    MAX (dac_member) as dac_member,
    'NA',
    'NA',
    MAX (unitcode) as unitcode,
    'NA',
    'NA',
    'NA'
    from
    disbursement_defl
    WHERE dac_member = 't'

    GROUP BY oda, year
    )tot_dac
  )ordered

  ORDER BY
  oda,
	  fao_region,
	  recipientcode,
	  donorcode,
	  parentsector_code,
	  purposecode,
	  fao_sector,
	  year,
	  channelsubcategory_code,
	  dac_member,
	  gaul0,
	  value,
	  unitcode,
	  flowcategory
)

