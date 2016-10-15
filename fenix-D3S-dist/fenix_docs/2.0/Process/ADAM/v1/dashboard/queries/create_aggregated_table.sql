
create table usd_aggregation_table_tot as(
  SELECT
  oda,
  year,
  value,
  parentsector_code,
  purposecode,
  recipientcode,
  dac_member,
  donorcode,
  unitcode,
  fao_sector,
  flowcategory,
  channelsubcategory_code,
  gaul0,
  fao_region,
  fao_subregion

  from

      (select
        'usd_disbursement' as oda,
        year,
        sum(value) as value,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,
          CASE WHEN purposecode in
          (
              '12240',
              '14030',
              '14031',
              '15170',
              '16062',
              '23070',
              '31110',
              '31120',
              '31130',
              '31140',
              '31150',
              '31161',
              '31162',
              '31163',
              '31164',
              '31165',
              '31166',
              '31181',
              '31182',
              '31191',
              '31192',
              '31193',
              '31194',
              '31195',
              '31210',
              '31220',
              '31261',
              '31281',
              '31282',
              '31291',
              '31310',
              '31320',
              '31381',
              '31382',
              '31391',
              '32161',
              '32162',
              '32163',
              '32165',
              '32267',
              '41010',
              '41020',
              '41030',
              '41040',
              '41050',
              '41081',
              '41082',
              '43040',
              '43050',
              '52010',
              '72040',
                                 '74010') THEN '1' ELSE '0' END as fao_sector,
                                 '10' as flowcategory,
                                 channelsubcategory_code,
      gaul0

        from usd_disbursement group by
        oda,
        year,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,
        channelsubcategory_code,
      gaul0

        union

      select
        'usd_disbursement_defl' as oda,
        year,
        sum(value) as value,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,

        CASE WHEN purposecode in (
              '12240',
              '14030',
              '14031',
              '15170',
              '16062',
              '23070',
              '31110',
              '31120',
              '31130',
              '31140',
              '31150',
              '31161',
              '31162',
              '31163',
              '31164',
              '31165',
              '31166',
              '31181',
              '31182',
              '31191',
              '31192',
              '31193',
              '31194',
              '31195',
              '31210',
              '31220',
              '31261',
              '31281',
              '31282',
              '31291',
              '31310',
              '31320',
              '31381',
              '31382',
              '31391',
              '32161',
              '32162',
              '32163',
              '32165',
              '32267',
              '41010',
              '41020',
              '41030',
              '41040',
              '41050',
              '41081',
              '41082',
              '43040',
              '43050',
              '52010',
              '72040',
                                 '74010') THEN '1' ELSE '0' END as fao_sector,
        '10' as flowcategory,
        channelsubcategory_code,
        gaul0

        from usd_disbursement_defl group by
        oda,
        year,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,
        channelsubcategory_code,
        gaul0

        union

        select
        'usd_commitment' as oda,
        year,
        sum(value) as value,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,

          CASE WHEN purposecode in (
              '12240',
              '14030',
              '14031',
              '15170',
              '16062',
              '23070',
              '31110',
              '31120',
              '31130',
              '31140',
              '31150',
              '31161',
              '31162',
              '31163',
              '31164',
              '31165',
              '31166',
              '31181',
              '31182',
              '31191',
              '31192',
              '31193',
              '31194',
              '31195',
              '31210',
              '31220',
              '31261',
              '31281',
              '31282',
              '31291',
              '31310',
              '31320',
              '31381',
              '31382',
              '31391',
              '32161',
              '32162',
              '32163',
              '32165',
              '32267',
              '41010',
              '41020',
              '41030',
              '41040',
              '41050',
              '41081',
              '41082',
              '43040',
              '43050',
              '52010',
              '72040',
                                 '74010') THEN '1' ELSE '0' END as fao_sector,
                                 '10' as flowcategory,
        channelsubcategory_code,
        gaul0

        from usd_commitment group by
        oda,
        year,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,
        channelsubcategory_code,
        gaul0

      union

      select
        'usd_commitment_defl' as oda,
        year,
        sum(value) as value,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,

          CASE WHEN purposecode in (
              '12240',
              '14030',
              '14031',
              '15170',
              '16062',
              '23070',
              '31110',
              '31120',
              '31130',
              '31140',
              '31150',
              '31161',
              '31162',
              '31163',
              '31164',
              '31165',
              '31166',
              '31181',
              '31182',
              '31191',
              '31192',
              '31193',
              '31194',
              '31195',
              '31210',
              '31220',
              '31261',
              '31281',
              '31282',
              '31291',
              '31310',
              '31320',
              '31381',
              '31382',
              '31391',
              '32161',
              '32162',
              '32163',
              '32165',
              '32267',
              '41010',
              '41020',
              '41030',
              '41040',
              '41050',
              '41081',
              '41082',
              '43040',
              '43050',
              '52010',
              '72040',
                                 '74010') THEN '1' ELSE '0' END as fao_sector,
                                 '10' as flowcategory,
        channelsubcategory_code,
        gaul0



        from usd_commitment_defl
        group by
        oda,
        year,
        parentsector_code,
        purposecode,
        recipientcode,
        dac_member,
        donorcode,
        unitcode,
        channelsubcategory_code,
        gaul0)t

        LEFT JOIN
        country_faoregion
        on
        t.recipientcode = country_faoregion.country
        )




