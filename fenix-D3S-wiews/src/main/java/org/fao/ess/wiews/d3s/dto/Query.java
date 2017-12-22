package org.fao.ess.wiews.d3s.dto;

public enum Query {

    indicator2 ("select * from indicators.indicator2" ),
    indicator3 ("select * from indicators.indicator3" ),
    indicator10 ("select * from indicators.indicator10" ),
    indicator14 ("select * from indicators.indicator14" ),
    indicator15 ("select * from indicators.indicator15" ),
    indicator16 ("select * from indicators.indicator16" ),

    indicator20 ("select * from indicators.indicator20" ),
    indicator22 ("select * from indicators.indicator22" ),
    indicator24 ("select * from indicators.indicator24" ),

    indicator28 ("select * from indicators.indicator28" ),

    raw_indicator2 ("WITH\n" +
            "    raw AS (\n" +
            "      SELECT a.questionid,\n" +
            "        a.approved,\n" +
            "        a.country_id,\n" +
            "        a.iteration,\n" +
            "        subquestionid,\n" +
            "        answerid,\n" +
            "        answer_freetext,\n" +
            "        reference_id,\n" +
            "        a.orgid\n" +
            "      FROM   answer a\n" +
            "        JOIN   answer_detail ad\n" +
            "          ON     (\n" +
            "          a.id = answerid )\n" +
            "      WHERE  questionid = 2\n" +
            "  )\n" +
            "\n" +
            "select\n" +
            "  iteration::text,\n" +
            "  ref_country.iso AS country_iso3,\n" +
            "  ref_country.name AS country,\n" +
            "  it.wiews_instcode::text as org_id,\n" +
            "  orgname_l as org_name,\n" +
            "  spec.answerid AS answer_id,\n" +
            "  species,\n" +
            "  threatened,\n" +
            "  varieties,\n" +
            "  threatened_varieties\n" +
            "from\n" +
            "  (select answerid, answer_freetext as species, iteration, country_id, orgid from raw where subquestionid = 1003) spec\n" +
            "  left join (select answerid, case when reference_id = '1001' then 'Yes' else 'No' end as threatened from raw where subquestionid = 1004) tspec\n" +
            "    on (spec.answerid = tspec.answerid)\n" +
            "  left join (select answerid, answer_freetext::int as varieties from raw where subquestionid = 1005) var\n" +
            "    on (spec.answerid = var.answerid)\n" +
            "  left join (select answerid, answer_freetext::int as threatened_varieties from raw where subquestionid = 1006) tvar\n" +
            "    on (spec.answerid = tvar.answerid)\n" +
            "\n" +
            "  left join ref_instab it on (it.id = spec.orgid)\n" +
            "  left join ref_country on (ref_country.country_id = spec.country_id and lang = 'EN')\n" +
            "\n" +
            "order by country, species"),

    raw_indicator3 ("WITH\n" +
            "    raw AS (\n" +
            "      SELECT a.questionid,\n" +
            "        a.approved,\n" +
            "        a.country_id,\n" +
            "        a.iteration,\n" +
            "        subquestionid,\n" +
            "        answerid,\n" +
            "        answer_freetext,\n" +
            "        reference_id,\n" +
            "        a.orgid\n" +
            "      FROM   answer a\n" +
            "        JOIN   answer_detail ad\n" +
            "          ON     (\n" +
            "          a.id = answerid )\n" +
            "      WHERE  questionid = 2\n" +
            "  )\n" +
            "\n" +
            "select\n" +
            "  iteration::text,\n" +
            "  ref_country.iso AS country_iso3,\n" +
            "  ref_country.name AS country,\n" +
            "  it.wiews_instcode::text as org_id,\n" +
            "  orgname_l as org_name,\n" +
            "  spec.answerid AS answer_id,\n" +
            "  species,\n" +
            "  threatened,\n" +
            "  varieties,\n" +
            "  threatened_varieties\n" +
            "from\n" +
            "  (select answerid, answer_freetext as species, iteration, country_id, orgid from raw where subquestionid = 1003) spec\n" +
            "  left join (select answerid, case when reference_id = '1001' then 'Yes' else 'No' end as threatened from raw where subquestionid = 1004) tspec\n" +
            "    on (spec.answerid = tspec.answerid)\n" +
            "  left join (select answerid, answer_freetext::int as varieties from raw where subquestionid = 1005) var\n" +
            "    on (spec.answerid = var.answerid)\n" +
            "  left join (select answerid, answer_freetext::int as threatened_varieties from raw where subquestionid = 1006) tvar\n" +
            "    on (spec.answerid = tvar.answerid)\n" +
            "\n" +
            "  left join ref_instab it on (it.id = spec.orgid)\n" +
            "  left join ref_country on (ref_country.country_id = spec.country_id and lang = 'EN')\n" +
            "\n" +
            "order by country, species"),

    raw_indicator10 (
            "SELECT\n" +
            "  iteration::text as iteration,\n" +
            "  c.iso as country_iso3,\n" +
            "  c.name as country,\n" +
            "  it.wiews_instcode::text                    AS org_id,\n" +
            "  orgname_l as org_name,\n" +
            "  a.id::text as answer_id,\n" +
            "  coalesce(t2.answer_freetext, '0') :: REAL AS sites_total,\n" +
            "  coalesce(t1.answer_freetext, '0') :: REAL AS sites_with_management\n" +
            "FROM answer a\n" +
            "  left JOIN answer_detail t1 ON (t1.answerId = a.id)\n" +
            "  left JOIN answer_detail t2 ON (t2.answerId = a.id)\n" +
            "  left JOIN ref_country c ON (c.country_id = a.country_id)\n" +
            "  left join ref_instab it on (it.id = a.orgid)\n" +
            "WHERE a.approved = 1 AND t1.subquestionid = 1043 AND t2.subquestionid = 1042"
    ),

    raw_indicator14 (
            "WITH\n" +
            "crops AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid, crop_id,\n" +
            "    coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "    answer_freetext_local AS crop_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "  WHERE c.subquestionid = 1058\n" +
            "),\n" +
            "enums AS (\n" +
            "  select answerid, subquestionid, string_agg(sl.option_value,'; ') AS value\n" +
            "  FROM\n" +
            "    answer_detail s\n" +
            "    LEFT JOIN ref_enum_options sl ON (s.reference_id = sl.id::text AND lang = 'EN')\n" +
            "  WHERE s.subquestionid IN (1057, 1059, 1062)\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "answers AS (\n" +
            "  select answerid, subquestionid, string_agg(answer_freetext,'; ') AS value\n" +
            "  FROM answer_detail\n" +
            "  WHERE subquestionid IN (1060, 1061, 1063, 1064)\n" +
            "  GROUP BY answerid, subquestionid\n" +
            ")\n" +
            "\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  c.iso                   AS country_iso3,\n" +
            "  c.name                  AS country,\n" +
            "  it.wiews_instcode::text AS org_id,\n" +
            "  orgname_l               AS org_name,\n" +
            "  a.id::text              AS answer_id,\n" +
            "  s.value                 AS strategy,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en      AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  gd.value                AS gaps,\n" +
            "  ogdl.value              AS other_gaps_local,\n" +
            "  ogd.value               AS other_gaps,\n" +
            "  m.value                 AS methods,\n" +
            "  oml.value               AS other_methods_local,\n" +
            "  om.value                AS other_methods\n" +
            "FROM answer a\n" +
            "  LEFT JOIN enums s ON (s.subquestionid = 1057 AND a.id = s.answerid)\n" +
            "  LEFT JOIN enums gd ON (gd.subquestionid = 1059 AND a.id = gd.answerid)\n" +
            "  LEFT JOIN answers ogdl ON (ogdl.subquestionid = 1060 AND a.id = ogdl.answerid)\n" +
            "  LEFT JOIN answers ogd ON (ogd.subquestionid = 1061 AND a.id = ogd.answerid)\n" +
            "  LEFT JOIN enums m ON (m.subquestionid = 1062 AND a.id = m.answerid)\n" +
            "  LEFT JOIN answers oml ON (oml.subquestionid = 1063 AND a.id = oml.answerid)\n" +
            "  LEFT JOIN answers om ON (om.subquestionid = 1064 AND a.id = om.answerid)\n" +
            "  LEFT JOIN ref_country c ON (a.country_id = c.country_id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "WHERE a.questionid = 11 AND a.approved = 1\n" +
            "ORDER BY a.iteration, c.iso, a.id"
    ),

    raw_indicator15 (
            "WITH\n" +
            "missions AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid,\n" +
            "    string_agg(project_code_l::text,'; ') AS mission_id,\n" +
            "    string_agg(coalesce(project_name_l, answer_freetext),'; ') AS mission_name,\n" +
            "    string_agg(answer_freetext_local,'; ') AS mission_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_protab rp ON (rp.id :: TEXT = c.reference_id::text)\n" +
            "  WHERE c.subquestionid = 1065\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "crops AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid, crop_id,\n" +
            "    coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "    answer_freetext_local AS crop_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "  WHERE c.subquestionid = 1068\n" +
            "),\n" +
            "taxons AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid,\n" +
            "    string_agg(taxon_id::text,'; ') AS taxon_id,\n" +
            "    string_agg(coalesce(taxon, answer_freetext),'; ') AS taxon_name,\n" +
            "    string_agg(answer_freetext_local,'; ') AS taxon_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_taxtab rt ON (taxon_id :: TEXT = c.reference_id::text)\n" +
            "  WHERE c.subquestionid = 1069\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "areas AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid,\n" +
            "    string_agg(area_id::text,'; ') AS area_id,\n" +
            "    string_agg(coalesce(area_l, answer_freetext),'; ') AS area_name,\n" +
            "    string_agg(answer_freetext_local,'; ') AS area_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_aretab ra ON (area_id :: TEXT = c.reference_id::text)\n" +
            "  WHERE c.subquestionid = 1072\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "answers AS (\n" +
            "  select answerid, subquestionid, answer_freetext AS value\n" +
            "  FROM answer_detail\n" +
            "  WHERE subquestionid IN (1066, 1067, 1070, 1071)\n" +
            ")\n" +
            "\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  co.iso                               AS country_iso3,\n" +
            "  co.name                              AS country,\n" +
            "  it.wiews_instcode::text              AS org_id,\n" +
            "  orgname_l                            AS org_name,\n" +
            "  a.id::text                           AS answer_id,\n" +
            "  mission_id,\n" +
            "  replace (mission_name,'\"','''')       AS mission_name,\n"+
            "  replace (mission_name_local,'\"','''') AS mission_name_local,\n"+
            "  t_start.value                        AS start_date,\n" +
            "  coalesce(t_end.value, t_start.value) AS end_date,\n" +
            "  crop_id,\n" +
            "  crop_name_en                         AS crop_name,\n" +
            "  crop_name_local,\n" +
            "  taxon_id,\n" +
            "  taxon_name_local,\n" +
            "  an.value::INTEGER                    AS accessions_number,\n" +
            "  ans.value::INTEGER                   AS secured_accessions_number,\n" +
            "  area_id,\n" +
            "  area_name,\n" +
            "  area_name_local\n" +
            "FROM answer a\n" +
            "  LEFT JOIN answers an ON (an.subquestionid = 1070 AND a.id = an.answerid)\n" +
            "  LEFT JOIN answers ans ON (ans.subquestionid = 1071 AND a.id = ans.answerid)\n" +
            "  LEFT JOIN answers t_start ON (t_start.subquestionid = 1066 AND a.id = t_start.answerid)\n" +
            "  LEFT JOIN answers t_end ON (t_end.subquestionid = 1067 AND a.id = t_end.answerid)\n" +
            "  LEFT JOIN missions ON (missions.answerId = a.id)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "  LEFT JOIN taxons ON (taxons.answerId = a.id)\n" +
            "  LEFT JOIN areas ON (areas.answerId = a.id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN ref_country co ON (co.country_id = a.country_id)\n" +
            "WHERE a.questionid = 12 AND a.approved = 1\n" +
            "ORDER BY a.iteration, co.iso, a.id"
    ),

    raw_indicator16 (
            "WITH\n" +
            "missions AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid,\n" +
            "    string_agg(project_code_l::text,'; ') AS mission_id,\n" +
            "    string_agg(coalesce(project_name_l, answer_freetext),'; ') AS mission_name,\n" +
            "    string_agg(answer_freetext_local,'; ') AS mission_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_protab rp ON (rp.id :: TEXT = c.reference_id::text)\n" +
            "  WHERE c.subquestionid = 1065\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "crops AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid, crop_id,\n" +
            "    coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "    answer_freetext_local AS crop_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "  WHERE c.subquestionid = 1068\n" +
            "),\n" +
            "taxons AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid,\n" +
            "    string_agg(taxon_id::text,'; ') AS taxon_id,\n" +
            "    string_agg(coalesce(taxon, answer_freetext),'; ') AS taxon_name,\n" +
            "    string_agg(answer_freetext_local,'; ') AS taxon_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_taxtab rt ON (taxon_id :: TEXT = c.reference_id::text)\n" +
            "  WHERE c.subquestionid = 1069\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "areas AS (\n" +
            "  SELECT\n" +
            "    c.answerid, c.subquestionid,\n" +
            "    string_agg(ra.id::text,'; ') AS area_id,\n" +
            "    string_agg(coalesce(area_l, answer_freetext),'; ') AS area_name,\n" +
            "    string_agg(answer_freetext_local,'; ') AS area_name_local\n" +
            "  FROM answer_detail c LEFT JOIN ref_aretab ra ON (area_id :: TEXT = c.reference_id::text)\n" +
            "  WHERE c.subquestionid = 1072\n" +
            "  GROUP BY answerid, subquestionid\n" +
            "),\n" +
            "answers AS (\n" +
            "  select answerid, subquestionid, answer_freetext AS value\n" +
            "  FROM answer_detail\n" +
            "  WHERE subquestionid IN (1066, 1067, 1070, 1071)\n" +
            ")\n" +
            "\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  co.iso                               AS country_iso3,\n" +
            "  co.name                              AS country,\n" +
            "  it.wiews_instcode::text              AS org_id,\n" +
            "  orgname_l                            AS org_name,\n" +
            "  a.id::text                           AS answer_id,\n" +
            "  mission_id,\n" +
            "  replace (mission_name,'\"','''')       AS mission_name,\n"+
            "  replace (mission_name_local,'\"','''') AS mission_name_local,\n"+
            "  t_start.value                        AS start_date,\n" +
            "  coalesce(t_end.value, t_start.value) AS end_date,\n" +
            "  crop_id,\n" +
            "  crop_name_en                         AS crop_name,\n" +
            "  crop_name_local,\n" +
            "  taxon_id,\n" +
            "  taxon_name_local,\n" +
            "  an.value::INTEGER                    AS accessions_number,\n" +
            "  ans.value::INTEGER                   AS secured_accessions_number,\n" +
            "  area_id,\n" +
            "  area_name,\n" +
            "  area_name_local\n" +
            "FROM answer a\n" +
            "  LEFT JOIN answers an ON (an.subquestionid = 1070 AND a.id = an.answerid)\n" +
            "  LEFT JOIN answers ans ON (ans.subquestionid = 1071 AND a.id = ans.answerid)\n" +
            "  LEFT JOIN answers t_start ON (t_start.subquestionid = 1066 AND a.id = t_start.answerid)\n" +
            "  LEFT JOIN answers t_end ON (t_end.subquestionid = 1067 AND a.id = t_end.answerid)\n" +
            "  LEFT JOIN missions ON (missions.answerId = a.id)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "  LEFT JOIN taxons ON (taxons.answerId = a.id)\n" +
            "  LEFT JOIN areas ON (areas.answerId = a.id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN ref_country co ON (co.country_id = a.country_id)\n" +
            "WHERE a.questionid = 12 AND a.approved = 1\n" +
            "ORDER BY a.iteration, co.iso, a.id"
    ),

    raw_indicator20 (
            "SELECT\n" +
            "  a.iteration::text as iteration,\n" +
            "  d.iso AS country_iso3,\n" +
            "  d.name::text as country,\n" +
            "  c.wiews_instcode::text as org_id,\n" +
            "  c.orgname_l::text as stakeholder,\n" +
            "  a.id::text as id,\n" +
            "  a.holdinginstitutecode::text as holdinginstitutecode,\n" +
            "  a.accessionno::text as accessionno,\n" +
            "  a.taxonid::text as taxonid,\n" +
            "  a.taxon_freetext::text as taxon_freetext,\n" +
            "  a.cropid::text as cropid,\n" +
            "  a.crop_freetext::text as crop_freetext,\n" +
            "  a.acquisitiondate::text as acquisitiondate,\n" +
            "  ref.name::text as origincountry,\n" +
            "  lb.wiews_codelist_title_en::text as biologicalaccession,\n" +
            "  a.genebankid::text as genebankid,\n" +
            "  a.genebank_freetext::text as genebank_freetext,\n" +
            "  a.latitude::text as latitude,\n" +
            "  a.longitude::text as longitude,\n" +
            "  lg.wiews_codelist_title_en::text as germplasmastore,\n" +
            "  lm.wiews_codelist_title_en::text as multilateralsystemstatus\n" +
            "\n" +
            "\n" +
            "FROM answer_q14 a\n" +
            "  LEFT JOIN ref_instab c on a.orgId = c.id\n" +
            "  LEFT JOIN ref_country d ON (a.country_id = d.country_id and d.lang = 'EN')\n" +
            "  LEFT JOIN ref_country ref on (a.countryoriginid = ref.country_id and ref.lang = 'EN')\n" +
            "  LEFT JOIN codelist.wiews_codelist lg on (lg.wiews_codelist_uid='wiews_germplasma' AND a.germplasmastoreid::text = lg.wiews_codelist_code)\n" +
            "  LEFT JOIN codelist.wiews_codelist lb on (lb.wiews_codelist_uid='wiews_biological' AND a.biologicalaccessionid::text = lb.wiews_codelist_code)\n" +
            "  LEFT JOIN codelist.wiews_codelist lm on (lm.wiews_codelist_uid='wiews_multilateral' AND a.multilateralsystemstatusid::text = lm.wiews_codelist_code)\n" +
            "ORDER BY a.iteration, d.iso, a.id"
    ),

    raw_indicator22 (
            "WITH crops AS (\n" +
            "    SELECT\n" +
            "      c.answerid,\n" +
            "      c.subquestionid,\n" +
            "      crop_id,\n" +
            "      coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "      answer_freetext_local AS crop_name_local\n" +
            "    FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "    WHERE c.subquestionid = 1086\n" +
            ")\n" +
            "\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  c.iso                                      AS country_iso3,\n" +
            "  c.name::text                               AS country,\n" +
            "  it.wiews_instcode::text                    AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  adn.answer_freetext :: REAL AS accessions_num,\n" +
            "  adr.answer_freetext :: REAL AS accessions_regenerated,\n" +
            "  adnr.answer_freetext :: REAL AS accessions_need_regeneration,\n" +
            "  ado.answer_freetext :: REAL AS accessions_out_of_budget\n" +
            "FROM answer a\n" +
            "  LEFT JOIN answer_detail ado ON (ado.subquestionid = 1090 AND a.id = ado.answerid)\n" +
            "  LEFT JOIN answer_detail adn ON (adn.subquestionid = 1087 AND a.id = adn.answerid)\n" +
            "  LEFT JOIN answer_detail adr ON (adr.subquestionid = 1088 AND a.id = adr.answerid)\n" +
            "  LEFT JOIN answer_detail adnr ON (adnr.subquestionid = 1089 AND a.id = adnr.answerid)\n" +
            "  LEFT JOIN ref_country c ON (a.country_id = c.country_id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "WHERE a.questionid = 15 AND a.approved = 1\n" +
            "ORDER BY a.iteration, c.iso, a.id"
    ),

    raw_indicator24 (
            "WITH crops AS (\n" +
            "    SELECT\n" +
            "      c.answerid,\n" +
            "      c.subquestionid,\n" +
            "      crop_id,\n" +
            "      coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "      answer_freetext_local AS crop_name_local\n" +
            "    FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "    WHERE c.subquestionid = 1086\n" +
            ")\n" +
            "\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  c.iso                                      AS country_iso3,\n" +
            "  c.name::text                               AS country,\n" +
            "  it.wiews_instcode::text                    AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  adn.answer_freetext :: REAL AS accessions_num,\n" +
            "  adr.answer_freetext :: REAL AS accessions_regenerated,\n" +
            "  adnr.answer_freetext :: REAL AS accessions_need_regeneration,\n" +
            "  ado.answer_freetext :: REAL AS accessions_out_of_budget\n" +
            "FROM answer a\n" +
            "  LEFT JOIN answer_detail ado ON (ado.subquestionid = 1090 AND a.id = ado.answerid)\n" +
            "  LEFT JOIN answer_detail adn ON (adn.subquestionid = 1087 AND a.id = adn.answerid)\n" +
            "  LEFT JOIN answer_detail adr ON (adr.subquestionid = 1088 AND a.id = adr.answerid)\n" +
            "  LEFT JOIN answer_detail adnr ON (adnr.subquestionid = 1089 AND a.id = adnr.answerid)\n" +
            "  LEFT JOIN ref_country c ON (a.country_id = c.country_id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "WHERE a.questionid = 15 AND a.approved = 1\n" +
            "ORDER BY a.iteration, c.iso, a.id"
    ),


    raw_indicator28 (
            "WITH crops AS (\n" +
            "    SELECT\n" +
            "      c.answerid,\n" +
            "      c.subquestionid,\n" +
            "      crop_id,\n" +
            "      coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "      answer_freetext_local AS crop_name_local\n" +
            "    FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "    WHERE c.subquestionid = 1102\n" +
            ")\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  c.iso                                      AS country_iso3,\n" +
            "  c.name::text                               AS country,\n" +
            "  it.wiews_instcode::text                    AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  adg.answer_freetext :: REAL AS accessions_by_genebank,\n" +
            "  adr.answer_freetext :: REAL AS accessions_by_research_centre,\n" +
            "  adp.answer_freetext :: REAL AS accessions_by_private_sector,\n" +
            "  adf.answer_freetext :: REAL AS accessions_by_farmer,\n" +
            "  ado.answer_freetext :: REAL AS accessions_by_other_national,\n" +
            "  ads.answer_freetext :: REAL AS accessions_by_stakeholder,\n" +
            "  adu.answer_freetext :: REAL AS accessions_by_unknown,\n" +
            "  sdg.answer_freetext :: REAL AS samples_by_genebank,\n" +
            "  sdr.answer_freetext :: REAL AS samples_by_research_centre,\n" +
            "  sdp.answer_freetext :: REAL AS samples_by_private_sector,\n" +
            "  sdf.answer_freetext :: REAL AS samples_by_farmer,\n" +
            "  sdo.answer_freetext :: REAL AS samples_by_other_national,\n" +
            "  sds.answer_freetext :: REAL AS samples_by_stakeholder,\n" +
            "  sdu.answer_freetext :: REAL AS samples_by_unknown\n" +
            "FROM\n" +
            "  answer a\n" +
            "  LEFT JOIN answer_detail adg ON (adg.subquestionid = 1103 AND a.id = adg.answerid)\n" +
            "  LEFT JOIN answer_detail adr ON (adr.subquestionid = 1104 AND a.id = adr.answerid)\n" +
            "  LEFT JOIN answer_detail adp ON (adp.subquestionid = 1105 AND a.id = adp.answerid)\n" +
            "  LEFT JOIN answer_detail adf ON (adf.subquestionid = 1106 AND a.id = adf.answerid)\n" +
            "  LEFT JOIN answer_detail ado ON (ado.subquestionid = 1107 AND a.id = ado.answerid)\n" +
            "  LEFT JOIN answer_detail ads ON (ads.subquestionid = 1108 AND a.id = ads.answerid)\n" +
            "  LEFT JOIN answer_detail adu ON (adu.subquestionid = 1109 AND a.id = adu.answerid)\n" +
            "  LEFT JOIN answer_detail sdg ON (sdg.subquestionid = 1110 AND a.id = sdg.answerid)\n" +
            "  LEFT JOIN answer_detail sdr ON (sdr.subquestionid = 1111 AND a.id = sdr.answerid)\n" +
            "  LEFT JOIN answer_detail sdp ON (sdp.subquestionid = 1112 AND a.id = sdp.answerid)\n" +
            "  LEFT JOIN answer_detail sdf ON (sdf.subquestionid = 1113 AND a.id = sdf.answerid)\n" +
            "  LEFT JOIN answer_detail sdo ON (sdo.subquestionid = 1114 AND a.id = sdo.answerid)\n" +
            "  LEFT JOIN answer_detail sds ON (sds.subquestionid = 1115 AND a.id = sds.answerid)\n" +
            "  LEFT JOIN answer_detail sdu ON (sdu.subquestionid = 1116 AND a.id = sdu.answerid)\n" +
            "  LEFT JOIN ref_country c ON (a.country_id = c.country_id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "WHERE a.questionid = 19 AND a.approved = 1\n" +
            "ORDER BY a.iteration, c.iso, a.id"
    ),


    wiews_organizations_pgsql (
            "SELECT\n" +
            "  replace(o.orgname_l,'\"','''') as name,\n" +
            "  replace(o.orgacro_l,'\"','''') as acronym,\n" +
            "  o.wiews_instcode as instcode,\n" +
            "  replace(po.orgname_l,'\"','''') as parent_name,\n" +
            "  po.wiews_instcode as parent_instcode,\n" +
            "  replace(o.address_l,'\"','''') as address,\n" +
            "  o.city_l as city,\n" +
            "  c.name as country,\n" +
            "  c.iso as country_iso3,\n" +
            "  o.valid_id = o.id as valid_flag,\n" +
            "  lower(replace(o.orgname_l,'\"','''')) as i_name,\n" +
            "  lower(replace(o.orgacro_l,'\"','''')) as i_acronym,\n" +
            "  lower(o.wiews_instcode) as i_instcode,\n" +
            "  lower(replace(o.address_l,'\"','''')) as i_address,\n" +
            "  lower(o.city_l) as i_city,\n" +
            "    trim (from coalesce(lower(replace(o.orgname_l,'\"',''''))||' ','')\n" +
            "    || coalesce(lower(replace(o.orgacro_l,'\"',''''))||' ','')\n" +
            "    || coalesce(lower(o.wiews_instcode)||' ','')\n" +
            "    || coalesce(lower(o.city_l)||' ','')\n" +
            "    || coalesce(lower(c.name)||' ','')) as index,\n" +
            "  o.zip as zip_code,\n" +
            "  o.phone as telephone,\n" +
            "  o.fax as fax,\n" +
            "  o.email as email,\n" +
            "  o.wwwaddress as website,\n" +
            "  s.option_value as status,\n" +
            "  o.longitude as longitude,\n" +
            "  o.latitude as latitude,\n" +
            "  CASE WHEN o.f646=1 OR o.f647=1 OR o.f648=1 OR o.f649=1 OR o.f650=1 OR o.f651=1 OR o.f652=1 OR o.f653=1 OR o.f654=1 OR o.f655=1 OR o.f656=1 OR o.f657=1 OR o.f658=1 OR o.f869=1 OR o.f874=1 OR o.f875=1 THEN substring(\n" +
            "    CASE WHEN o.f646=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f646') ELSE '' END ||\n" +
            "    CASE WHEN o.f647=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f647') ELSE '' END ||\n" +
            "    CASE WHEN o.f648=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f648') ELSE '' END ||\n" +
            "    CASE WHEN o.f649=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f649') ELSE '' END ||\n" +
            "    CASE WHEN o.f650=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f650') ELSE '' END ||\n" +
            "    CASE WHEN o.f651=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f651') ELSE '' END ||\n" +
            "    CASE WHEN o.f652=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f652') ELSE '' END ||\n" +
            "    CASE WHEN o.f653=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f653') ELSE '' END ||\n" +
            "    CASE WHEN o.f654=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f654') ELSE '' END ||\n" +
            "    CASE WHEN o.f655=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f655') ELSE '' END ||\n" +
            "    CASE WHEN o.f656=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f656') ELSE '' END ||\n" +
            "    CASE WHEN o.f657=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f657') ELSE '' END ||\n" +
            "    CASE WHEN o.f658=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f658') ELSE '' END ||\n" +
            "    CASE WHEN o.f869=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f869') ELSE '' END ||\n" +
            "    CASE WHEN o.f874=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f874') ELSE '' END ||\n" +
            "    CASE WHEN o.f875=1 THEN (SELECT '; '||label_en FROM codelist.organizations_role WHERE code = 'f875') ELSE '' END\n" +
            "    FROM 3) ELSE NULL END AS organization_roles,\n" +
            "  vo.wiews_instcode AS valid_instcode,\n" +
            "  o.f646 = 1 as role_f646,\n" +
            "  o.f647 = 1 as role_f647,\n" +
            "  o.f648 = 1 as role_f648,\n" +
            "  o.f649 = 1 as role_f649,\n" +
            "  o.f650 = 1 as role_f650,\n" +
            "  o.f651 = 1 as role_f651,\n" +
            "  o.f652 = 1 as role_f652,\n" +
            "  o.f653 = 1 as role_f653,\n" +
            "  o.f654 = 1 as role_f654,\n" +
            "  o.f655 = 1 as role_f655,\n" +
            "  o.f656 = 1 as role_f656,\n" +
            "  o.f657 = 1 as role_f657,\n" +
            "  o.f658 = 1 as role_f658,\n" +
            "  o.f869 = 1 as role_f869,\n" +
            "  o.f874 = 1 as role_f874,\n" +
            "  o.f875 = 1 as role_f875\n" +
            "FROM ref_instab o\n" +
            "  LEFT JOIN ref_instab po ON (o.parent_id IS NOT NULL AND o.parent_id != o.id AND o.parent_id = po.id)\n" +
            "  LEFT JOIN ref_instab vo ON (o.valid_id IS NOT NULL AND o.valid_id = vo.id)\n" +
            "  LEFT JOIN ref_country c ON (o.country_id = c.country_id and c.lang = 'EN')\n" +
            "  LEFT JOIN ref_enum_options s ON (o.orgstatus = s.id and s.lang = 'EN')"
    ),
    wiews_organizations (
            "SELECT\n" +
            "  replace(o.orgname_l,'\\\"','''') as name,\n" +
            "  replace(o.orgacro_l,'\\\"','''') as acronym,\n" +
            "  o.wiews_instcode as instcode,\n" +
            "  replace(po.orgname_l,'\\\"','''') as parent_name,\n" +
            "  po.wiews_instcode as parent_instcode,\n" +
            "  replace(o.address_l,'\\\"','''') as address,\n" +
            "  o.city_l as city,\n" +
            "  c.name as country,\n" +
            "  c.iso as country_iso3,\n" +
            "  o.valid_id = o.id as valid_flag,\n" +
            "  lower(replace(o.orgname_l,'\\\"','''')) as i_name,\n" +
            "  lower(replace(o.orgacro_l,'\\\"','''')) as i_acronym,\n" +
            "  lower(o.wiews_instcode) as i_instcode,\n" +
            "  lower(replace(o.address_l,'\\\"','''')) as i_address,\n" +
            "  lower(o.city_l) as i_city,\n" +
            "  trim(concat(coalesce(concat(lower(replace(o.orgname_l,'\\\"','''')),' '),'')\n" +
            "             , coalesce(concat(lower(replace(o.orgacro_l,'\\\"','''')),' '),'')\n" +
            "             , coalesce(concat(lower(o.wiews_instcode),' '),'')\n" +
            "             , coalesce(concat(lower(o.city_l),' '),'')\n" +
            "             , coalesce(concat(lower(c.name),' '),''))) AS ordering_index,\n" +
            "  o.zip as zip_code,\n" +
            "  o.phone as telephone,\n" +
            "  o.fax as fax,\n" +
            "  o.email as email,\n" +
            "  o.wwwaddress as website,\n" +
            "  s.option_value as status,\n" +
            "  o.longitude as longitude,\n" +
            "  o.latitude as latitude,\n" +
            "  CASE WHEN o.f646=1 OR o.f647=1 OR o.f648=1 OR o.f649=1 OR o.f650=1 OR o.f651=1 OR o.f652=1 OR o.f653=1 OR o.f654=1 OR o.f655=1 OR o.f656=1 OR o.f657=1 OR o.f658=1 OR o.f869=1 OR o.f874=1 OR o.f875=1 THEN substring(\n" +
            "  CASE WHEN o.f646=1 THEN '; Genebank (long term collections)' ELSE '' END ||\n" +
            "  CASE WHEN o.f647=1 THEN '; Botanical garden' ELSE '' END ||\n" +
            "  CASE WHEN o.f648=1 THEN '; Breeder' ELSE '' END ||\n" +
            "  CASE WHEN o.f649=1 THEN '; Network' ELSE '' END ||\n" +
            "  CASE WHEN o.f650=1 THEN '; Community' ELSE '' END ||\n" +
            "  CASE WHEN o.f651=1 THEN '; Educational' ELSE '' END ||\n" +
            "  CASE WHEN o.f652=1 THEN '; Seed producer' ELSE '' END ||\n" +
            "  CASE WHEN o.f653=1 THEN '; Seed supplier' ELSE '' END ||\n" +
            "  CASE WHEN o.f654=1 THEN '; Farmer community' ELSE '' END ||\n" +
            "  CASE WHEN o.f655=1 THEN '; Research' ELSE '' END ||\n" +
            "  CASE WHEN o.f656=1 THEN '; Extensionist' ELSE '' END ||\n" +
            "  CASE WHEN o.f657=1 THEN '; Laboratory' ELSE '' END ||\n" +
            "  CASE WHEN o.f658=1 THEN '; Publisher' ELSE '' END ||\n" +
            "  CASE WHEN o.f869=1 THEN '; Administration/Policy' ELSE '' END ||\n" +
            "  CASE WHEN o.f874=1 THEN '; Genebank (medium term collections)' ELSE '' END ||\n" +
            "  CASE WHEN o.f875=1 THEN '; Genebank (short term collections)' ELSE '' END\n" +
            "  FROM 3) ELSE NULL END AS organization_roles,\n" +
            "  vo.wiews_instcode AS valid_instcode,\n" +
            "  o.f646 = 1 as role_f646,\n" +
            "  o.f647 = 1 as role_f647,\n" +
            "  o.f648 = 1 as role_f648,\n" +
            "  o.f649 = 1 as role_f649,\n" +
            "  o.f650 = 1 as role_f650,\n" +
            "  o.f651 = 1 as role_f651,\n" +
            "  o.f652 = 1 as role_f652,\n" +
            "  o.f653 = 1 as role_f653,\n" +
            "  o.f654 = 1 as role_f654,\n" +
            "  o.f655 = 1 as role_f655,\n" +
            "  o.f656 = 1 as role_f656,\n" +
            "  o.f657 = 1 as role_f657,\n" +
            "  o.f658 = 1 as role_f658,\n" +
            "  o.f869 = 1 as role_f869,\n" +
            "  o.f874 = 1 as role_f874,\n" +
            "  o.f875 = 1 as role_f875\n" +
            "FROM instab o\n" +
            "LEFT JOIN instab po ON (o.parent_id IS NOT NULL AND o.parent_id != o.id AND o.parent_id = po.id)\n" +
            "LEFT JOIN instab vo ON (o.valid_id IS NOT NULL AND o.valid_id = vo.id)\n" +
            "LEFT JOIN country c ON (o.country_id = c.country_id and c.lang = 'EN')\n" +
            "LEFT JOIN question_type_enum_options s ON (o.orgstatus = s.id and s.lang = 'EN')"
    ),

    wiews_region_mapping(
            "SELECT w, fao, m49, mdg, sdg, cgrfa, itpgrfa, rank FROM codelist.region_mapping"
    ),

    wiews_regions_mapping(
            "SELECT iso3_country_code, faol0_code, faol1_code, faol2_code, m49l0_code, m49l1_code, m49l2_code, m49_country_code, mdg_region_code, sdg_region_code, itpgrfa_region_code, cgrfa_region_code FROM codelist.regional_mapping"
    ),

    wiews_region_countries(
            "SELECT w, country_iso3 FROM codelist.region_countries"
    ),


    //SDG
    exsitu_institutes_count(
            "SELECT year,\n" +
            "  country_iso3,\n" +
            "  w_instcode,\n" +
            "  (CASE WHEN latitude SIMILAR TO '\\d+.\\d+' THEN latitude ELSE NULL END)::DOUBLE PRECISION AS latitude,\n" +
            "  (CASE WHEN longitude SIMILAR TO '\\d+.\\d+' THEN longitude ELSE NULL END)::DOUBLE PRECISION AS longitude,\n" +
            "  accessions_count,\n" +
            "  genus_count,\n" +
            "  genus_species_count\n" +
            "FROM (\n" +
            "  SELECT\n" +
            "    2016 as year,\n" +
            "    nicode as country_iso3,\n" +
            "    w_instcode,\n" +
            "    count(DISTINCT accenumb) AS accessions_count,\n" +
            "    count(DISTINCT genus) AS genus_count,\n" +
            "    count(DISTINCT genus||'|'||species) AS genus_species_count\n" +
            "  FROM sdg.wiews_2016\n" +
            "  GROUP BY nicode, w_instcode\n" +
            "  UNION\n" +
            "  SELECT\n" +
            "    2014 as year,\n" +
            "    nicode as country_iso3,\n" +
            "    w_instcode,\n" +
            "    count(DISTINCT accenumb) AS accessions_count,\n" +
            "    count(DISTINCT genus) AS genus_count,\n" +
            "    count(DISTINCT genus||'|'||species) AS genus_species_count\n" +
            "  FROM sdg.wiews_2014\n" +
            "  GROUP BY nicode, w_instcode\n" +
            ") raw\n" +
            "  LEFT JOIN ref_instab ON (raw.w_instcode=ref_instab.wiews_instcode)"
    ),


    exsitu_index_country(
            "SELECT 2016 AS year, w_instcode, nicode AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item\n" +
            "UNION ALL\n" +
            "SELECT 2014 AS year, w_instcode, nicode AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item"
    ),

    exsitu_index_taxon(
            "SELECT 2016 AS year, w_instcode, taxon AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item\n" +
            "UNION ALL\n" +
            "SELECT 2014 AS year, w_instcode, taxon AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item"
    ),

    exsitu_index_specie(
            "SELECT 2016 AS year, w_instcode, genus||'|'||species AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item\n" +
            "UNION ALL\n" +
            "SELECT 2014 AS year, w_instcode, genus||'|'||species AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item"
    ),

    exsitu_index_genus(
            "SELECT 2016 AS year, w_instcode, genus AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item\n" +
            "UNION ALL\n" +
            "SELECT 2014 AS year, w_instcode, genus AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item"
    ),

    exsitu_index_crop(
            "SELECT 2016 AS year, w_instcode, cropname AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item\n" +
            "UNION ALL\n" +
            "SELECT 2014 AS year, w_instcode, cropname AS item FROM sdg.wiews_2016 GROUP BY w_instcode, item"
    ),


    ;private String query;
    Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


