create table browse_donor_recipient as (
   select
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      fao_sector,
      recipientcode,
      year,
      sum(value) as value,
      max(unitcode) as unitcode,
      gaul0,
      flowcategory
   from
      usd_aggregated_table2
   group by
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      fao_sector,
      recipientcode,
      year,
      gaul0,
      flowcategory
   order by
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      fao_sector,
      recipientcode,
      year,
      gaul0,
      flowcategory
);