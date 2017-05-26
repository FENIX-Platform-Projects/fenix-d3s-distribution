create table resource_matrix_oda as (
	select
	oda,
	donorcode,
	recipientcode,
	fao_region,
	year,
	fao_sector,
	sum(value) as value,
	max(unitcode) as unitcode
	from usd_aggregated_table
	WHERE donorcode != 'NA'
	group by
	oda,
	donorcode,
	recipientcode,
	fao_region,
	year,
	fao_sector
	order by
	oda,
	donorcode,
	recipientcode,
	fao_region,
	year,
	fao_sector
