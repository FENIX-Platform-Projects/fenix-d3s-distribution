create table browse_recipient_oda as (
   select
      oda,
      fao_region,
      recipientcode,
      gaul0,
      parentsector_code,
      purposecode,
      channelsubcategory_code,
      year,
      fao_sector,
      flowcategory,
      sum(value) as value,
      max(unitcode) as unitcode
   from
      usd_aggregated_table
   group by
     oda,
      fao_region,
      recipientcode,
      gaul0,
      parentsector_code,
      purposecode,
      channelsubcategory_code,
      year,
      fao_sector,
      flowcategory

   order by
      oda,
      fao_region,
      recipientcode,
      gaul0,
      parentsector_code,
      purposecode,
      year,
      fao_sector,
      flowcategory
);