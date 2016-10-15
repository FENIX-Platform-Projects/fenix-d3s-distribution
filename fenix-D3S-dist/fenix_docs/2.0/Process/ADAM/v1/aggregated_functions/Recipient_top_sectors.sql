
with commitment as (SELECT * from (

		select
		parentsector_code,
    purposecode,
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
                               cast('usd_commitment' as text)as oda,
    year,
    recipientcode,
    donorcode,
    flowcategory_code,
    sum(value) as value,
    max(unitcode) as unitcode
    FROM
    usd_commitment
    GROUP BY
    parentsector_code,
    purposecode,
    year,
    recipientcode,
    donorcode,
    flowcategory_code
   )t LEFT JOIN
    country_faoregion
    ON t.recipientcode = country_faoregion.country
),

comm_defl as (
SELECT * from (

	select
		parentsector_code,
    purposecode,
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
                               cast('usd_commitment_defl' as text)as oda,
    year,
    recipientcode,
    donorcode,
    flowcategory_code,
    sum(value) as value,
    max(unitcode) as unitcode
    FROM
    usd_commitment_defl
    GROUP BY
    parentsector_code,
    purposecode,
    year,
    recipientcode,
    donorcode,
    flowcategory_code
   )t LEFT JOIN
    country_faoregion
    ON t.recipientcode = country_faoregion.country
),
disb as (
SELECT * from (

	select
		parentsector_code,
    purposecode,
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
                               cast('usd_disbursement' as text)as oda,
    year,
    recipientcode,
    donorcode,
    flowcategory_code,
    sum(value) as value,
    max(unitcode) as unitcode
    FROM
    usd_disbursement
    GROUP BY
    parentsector_code,
    purposecode,
    year,
    recipientcode,
    donorcode,
    flowcategory_code
   )t LEFT JOIN
    country_faoregion
    ON t.recipientcode = country_faoregion.country
),
disb_defl as (
SELECT * from (

	select
		parentsector_code,
    purposecode,
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
                               cast('usd_disbursement_defl' as text)as oda,
    year,
    recipientcode,
    donorcode,
    flowcategory_code,
    sum(value) as value,
    max(unitcode) as unitcode
    FROM
    usd_disbursement_defl
    GROUP BY
    parentsector_code,
    purposecode,
    year,
    recipientcode,
    donorcode,
    flowcategory_code
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
-- 2410366 rows






