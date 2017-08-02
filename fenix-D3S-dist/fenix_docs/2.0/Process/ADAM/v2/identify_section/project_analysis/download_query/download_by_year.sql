with constant as(
  SELECT
    CASE WHEN test_region='XXX' then null ELSE test_region END as fao_region,
    CASE WHEN test_region_en='XXX' then null ELSE test_region_en END as region,
    recipientcode,
    recipientcode_en AS recipient,
    donorcode,
    donorcode_en AS donor,
    projecttitle,
    year,
    parentsector_code,
    parentsector_code_en AS parentsector,
    fao_sector,
    purposecode,
    purposecode_en AS purpose,
    a.value AS comm_val,
    b.value AS disb_val
  FROM (SELECT *,
          coalesce(fao_region,'XXX'):: TEXT as test_region,
          coalesce(fao_region_en,'XXX'):: TEXT as test_region_en
        FROM data.adam_project_analysis_v2
        WHERE oda IN ('usd_commitment')) a
    FULL JOIN (SELECT *,
                coalesce(fao_region,'XXX'):: TEXT as test_region,
                coalesce(fao_region_en,'XXX'):: TEXT as test_region_en
               FROM data.adam_project_analysis_v2
               WHERE oda IN ('usd_disbursement')) b
    USING (test_region,test_region_en,recipientcode,recipientcode_en, donorcode,donorcode_en, parentsector_code,parentsector_code_en,fao_sector, purposecode, purposecode_en,year, projecttitle)
),

/**************************************
DEFL
**************************************/

defl as (
SELECT
    CASE WHEN test_region='XXX' then null ELSE test_region END as fao_region,
    CASE WHEN test_region_en='XXX' then null ELSE test_region_en END as region,
    recipientcode,
    recipientcode_en AS recipient,
    donorcode,
    donorcode_en AS donor,
    projecttitle,
    year,
    parentsector_code,
    parentsector_code_en AS parentsector,
    fao_sector,
    purposecode,
    purposecode_en AS purpose,
    a.value AS comm_defl_val,
    b.value AS disb_defl_val
  FROM (SELECT *,
          coalesce(fao_region,'XXX'):: TEXT as test_region,
          coalesce(fao_region_en,'XXX'):: TEXT as test_region_en
        FROM data.adam_project_analysis_v2
        WHERE oda IN ('usd_commitment_defl')) a
    FULL JOIN (SELECT *,
                coalesce(fao_region,'XXX'):: TEXT as test_region,
                coalesce(fao_region_en,'XXX'):: TEXT as test_region_en
               FROM data.adam_project_analysis_v2
               WHERE oda IN ('usd_disbursement_defl')) b
    USING (test_region,test_region_en,recipientcode,recipientcode_en, donorcode,donorcode_en, parentsector_code,parentsector_code_en,fao_sector, purposecode, purposecode_en,year, projecttitle)
)

SELECT
  fao_region,
  region,
  recipientcode,
  recipient,
  donorcode,
  donor,
  projecttitle,
  year,
  parentsector_code,
  parentsector,
  fao_sector,
  purposecode,
  purpose,
  a.comm_val as commitment_current,
  a.disb_val as disbursement_current,
  b.comm_defl_val as commitment_deflated,
  b.disb_defl_val as disbursement_deflated,
  'million_usd'::TEXT as unitcode,
  'Million USD'::TEXT as unit

FROM constant a FULL JOIN defl b USING (fao_region,  region,recipientcode, recipient, donorcode, donor, projecttitle, year, parentsector_code, parentsector, fao_sector, purposecode ,purpose)