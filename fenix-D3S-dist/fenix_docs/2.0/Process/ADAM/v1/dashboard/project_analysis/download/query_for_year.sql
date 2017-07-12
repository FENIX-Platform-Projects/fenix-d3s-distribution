SELECT
  oda_grp_en           AS CURRENT,
  recipientcode,
  recipientcode_en     AS recipient,
  donorcode,
  donorcode_en         AS donor,
  projecttitle,
  year,
  parentsector_code,
  parentsector_code_en AS parentsector,
  purposecode,
  purposecode_en       AS purpose,
  commitment_value,
  disbursement_value,
  unitcode,
  unitcode_en          AS unit
FROM data.adam_project_analysis
WHERE year BETWEEN 2000 and 2015
ORDER BY
  year,
  recipient,
  donor,
  parentsector,
  purpose;