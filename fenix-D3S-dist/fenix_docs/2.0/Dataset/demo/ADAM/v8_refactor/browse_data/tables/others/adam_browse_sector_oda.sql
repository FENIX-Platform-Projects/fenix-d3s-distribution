 create table browse_sector_oda as (
select oda,
donorcode,
parentsector_code,
purposecode,
 year,
 dac_member,
 fao_sector,
 flowcategory,
sum(value) as value,
max(unitcode) as unitcode
 from usd_aggregated_table
 group by
 oda,
donorcode,
parentsector_code,
purposecode,
 year,
 dac_member,
 fao_sector,
 flowcategory
 order by
 oda,
donorcode,
parentsector_code,
purposecode,
 year,
 dac_member,
 fao_sector,
 flowcategory);