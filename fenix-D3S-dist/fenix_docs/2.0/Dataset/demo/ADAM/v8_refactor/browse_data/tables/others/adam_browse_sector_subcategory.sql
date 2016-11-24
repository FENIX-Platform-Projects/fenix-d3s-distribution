create table browse_sector_subcategory as (
select
oda,
parentsector_code,
purposecode,
channelsubcategory_code,
year,
fao_sector,
flowcategory,
sum(value) as value,
max(unitcode) as unitcode
 from usd_aggregated_table
 group by
oda,
parentsector_code,
purposecode,
channelsubcategory_code,
year,
fao_sector,
flowcategory
order by
oda,
parentsector_code,
purposecode,
channelsubcategory_code,
year,
fao_sector,
flowcategory);