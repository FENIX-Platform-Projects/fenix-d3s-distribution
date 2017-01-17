create table browse_recipient_donor as (
   select
      oda,
      fao_region,
      recipientcode,
      parentsector_code,
      purposecode,
      donorcode,
      year,
      fao_sector,
      sum(value) as value,
      max(unitcode) as unitcode,
      gaul0,
      flowcategory
   from
      usd_aggregated_table
   group by
      oda,
      fao_region,
      recipientcode,
      parentsector_code,
      purposecode,
      donorcode,
      year,
      fao_sector,
      gaul0,
      flowcategory
   order by
      oda,
      fao_region,
      recipientcode,
      parentsector_code,
      purposecode,
      donorcode,
      year,
      fao_sector,
      gaul0,
      flowcategory
);