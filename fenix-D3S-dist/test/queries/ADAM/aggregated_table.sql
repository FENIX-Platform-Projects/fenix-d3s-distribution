insert into usd_aggregation_table (
    oda,
    year,
    value,
    sectorcode,
    sectorname,
    purposecode,
    purposename,
    recipientcode,
    recipientname,
    dac_member,
    donorcode,
    donorname,
    unitcode,
    unitname
    )

     select
     'usd_commitment' as oda,
      year,
      sum(value) as value,
      sectorcode,
      sectorname,
      purposecode,
      purposename,
      recipientcode,
      recipientname,
      dac_member,
      donorcode,
      donorname,
      unitcode,
      unitname

      from usd_commitment

      group by
      year,
      sectorcode,
      purposecode,
      recipientcode,
      dac_member,
      donorcode,
      unitcode,
      sectorname,
      purposename,
      recipientname,
      donorname,
      unitname,
      oda