create table browse_donor_faoregion as (
   select
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      fao_sector,
      fao_region,
      year,
      sum(value) as value,
      max(unitcode) as unitcode,
      gaul0,
      flowcategory
   from
      usd_aggregated_table
   group by
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      fao_sector,
      fao_region,
      year,
      gaul0,
      flowcategory
   order by
      oda,
      parentsector_code,
      purposecode,
      donorcode,
      fao_sector,
      fao_region,
      year,
      gaul0,
      flowcategory
);