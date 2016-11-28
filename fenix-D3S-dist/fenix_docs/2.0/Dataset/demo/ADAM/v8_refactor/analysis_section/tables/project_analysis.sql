CREATE TABLE project_analysis2 as (

SELECT
  oda,
  fao_region,
  recipientcode,
  donorcode,
  fao_sector,
  parentsector_code,
  purposecode,
  year,
  projecttitle,
  sum(value) as VALUE ,
  max(unitcode) as unitcode

  from
    (
      SELECT
      projecttitle,
      year,
      recipientcode,
      donorcode,
      parentsector_code,
      purposecode,
      sum(value) as VALUE ,
      max(unitcode) as unitcode,
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
      'Commitment (USD Mil)' as oda


    from
      usd_commitment
    GROUP BY
      projecttitle,
      year,
      recipientcode,
      donorcode,
      parentsector_code,
      purposecode

    UNION

    SELECT
      projecttitle,
      year,
      recipientcode,
      donorcode,
      parentsector_code,
      purposecode,
      sum(value) as VALUE ,
      max(unitcode) as unitcode,
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
      'Disbursement (USD Mil)' as oda


    from
      usd_disbursement
    GROUP BY
      projecttitle,
      year,
      recipientcode,
      donorcode,
      parentsector_code,
      purposecode
      )t
      LEFT JOIN
      faoregion
      ON t.recipientcode = faoregion.country


    group BY
    oda,
    fao_region,
    recipientcode,
    donorcode,
    fao_sector,
    parentsector_code,
    purposecode,
    year,
    projecttitle

    order BY
    oda,
    fao_region,
    recipientcode,
    donorcode,
    fao_sector,
    parentsector_code,
    purposecode,
    year,
    projecttitle
)