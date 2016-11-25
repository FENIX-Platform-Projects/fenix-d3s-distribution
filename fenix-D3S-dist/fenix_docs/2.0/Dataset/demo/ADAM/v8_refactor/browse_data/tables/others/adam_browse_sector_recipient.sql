 create table browse_sector_recipient as (
select
oda,
parentsector_code,
purposecode,
recipientcode,
fao_region,
 year,
 fao_sector,
 flowcategory,
sum(value) as value,
max(unitcode) as unitcode,
gaul0
 from usd_aggregated_table
 group by
 oda,
parentsector_code,
purposecode,
recipientcode,
fao_region,

 year,
 fao_sector,
 flowcategory,
 gaul0
 order by
 oda,
parentsector_code,
purposecode,
recipientcode,
fao_region,

 year,
 fao_sector,
 flowcategory,
 gaul0);