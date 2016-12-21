create table compare_analysis as (
   select
      oda,
      donorcode,
      recipientcode,
      parentsector_code,
      purposecode,
      year,
      fao_sector,
      sum(value) as value,
      max(unitcode) as unitcode

   from
      usd_aggregated_table

   WHERE
    donorcode != 'NA' AND
    recipientcode != 'NA' AND
    parentsector_code != 'NA' AND
    purposecode  != 'NA' AND
    fao_sector != 'NA'
   group by
      oda,
      donorcode,
      recipientcode,
      parentsector_code,
      purposecode,
      year,
      fao_sector,
   order by
      oda,
      donorcode,
      recipientcode,
      parentsector_code,
      purposecode,
      year,
      fao_sector
);