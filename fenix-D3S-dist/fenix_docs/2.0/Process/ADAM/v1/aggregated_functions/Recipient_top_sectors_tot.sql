
with commitment as (
SELECT * from (

	 SELECT * from(
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
      country_faoregion
      ON t.recipientcode = country_faoregion.country
),

comm_defl as (
SELECT * from (

select
   parentsector_code,
   cast('usd_commitment_defl' as text) as oda,
   fao_sector,
   recipientcode,
   year,
   sum(value) as value,
   max(unitcode) as unitcode
   FROM (
      select
      parentsector_code,
      recipientcode,
      year,
      CASE
         WHEN purposecode in (           '12240',
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
         '74010') THEN '1'
         ELSE '0'
      END as fao_sector,
      value,
      unitcode
   from
      usd_commitment_defl )z
GROUP BY
   parentsector_code,
   fao_sector,
   recipientcode,
   year)t LEFT JOIN
    country_faoregion
    ON t.recipientcode = country_faoregion.country
),
disb as (
SELECT * from (
select
   parentsector_code,
   cast('usd_disbursement' as text) as oda,
   fao_sector,
   recipientcode,
   year,
   sum(value) as value,
   max(unitcode) as unitcode
   FROM (
      select
      parentsector_code,
      recipientcode,
      year,
      CASE
         WHEN purposecode in (           '12240',
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
         '74010') THEN '1'
         ELSE '0'
      END as fao_sector,
      value,
      unitcode
   from
      usd_disbursement )z
GROUP BY
   parentsector_code,
   fao_sector,
   recipientcode,
   year )t LEFT JOIN
    country_faoregion
    ON t.recipientcode = country_faoregion.country
),
disb_defl as (

SELECT * from (

select
   parentsector_code,
   cast('usd_disbursement' as text) as oda,
   fao_sector,
   recipientcode,
   year,
   sum(value) as value,
   max(unitcode) as unitcode
   FROM (
      select
      parentsector_code,
      recipientcode,
      year,
      CASE
         WHEN purposecode in (           '12240',
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
         '74010') THEN '1'
         ELSE '0'
      END as fao_sector,
      value,
      unitcode
   from
      usd_disbursement )z
GROUP BY
   parentsector_code,
   fao_sector,
   recipientcode,
   year
   )t LEFT JOIN
    country_faoregion
    ON t.recipientcode = country_faoregion.country
)



	drop table recipient_top_sectors;

	CREATE TABLE recipient_top_sectors as (
	select * from commitment
	    UNION
	select * from comm_defl
	    UNION
	select * from disb
	    UNION
	select * from disb_defl
)
-- 191970





with disbursement as(

  SELECT * from(
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
      country_faoregion
      ON t.recipientcode = country_faoregion.country
),


disbursement_defl as (
  SELECT * from(
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
      country_faoregion
      ON t.recipientcode = country_faoregion.country
),

commitment as (

  SELECT * from(
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
      country_faoregion
      ON t.recipientcode = country_faoregion.country
),

commitment_defl as (

  SELECT * from(
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
      country_faoregion
      ON t.recipientcode = country_faoregion.country
)


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



  )ordered

  ORDER BY oda,channelsubcategory_code,YEAR ,parentsector_code
  limit 10









