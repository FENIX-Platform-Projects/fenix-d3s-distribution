SELECT
  recipientcode,
  recipientcode_en as recipientcode_label,
  oda,
  donorcode,
  donorcode_en as donorcode_label,
  projecttitle,
  year,
  parentsector_code,
  parentsector_code_en as parentsector_code_label,
  purposecode,
  parentsector_code_en as purposecode_label,
  value,
  unitcode,
  unitcode_en as unitcode_label
FROM data.adam_project_analysis
WHERE recipientcode not in (
  '298',
  '498',
  '798',
  '89',
  '589',
  '889',
  '189',
  '289',
  '389',
  '380',
  '489',
  '789',
  '689',
  '619',
  '679')
ORDER BY
year,
recipientcode_label,
donorcode_label,
parentsector_code_label,
purposecode_label;