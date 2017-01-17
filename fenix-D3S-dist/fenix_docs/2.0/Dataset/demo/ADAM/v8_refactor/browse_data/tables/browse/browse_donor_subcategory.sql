create table browse_donor_subcategory as (
   select
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      channelsubcategory_code,
      year,
      fao_sector,
      sum(value) as value,
      max(unitcode) as unitcode,
      flowcategory
   from
      usd_aggregated_table
   group by
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      channelsubcategory_code,
      year,
      fao_sector,
      flowcategory

   order by
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      channelsubcategory_code,
      year,
      fao_sector,
      flowcategory
);