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
            "  left join ref_instab on (ref_instab.id = spec.orgid)\n" +
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
            "  left join ref_instab on (ref_instab.id = spec.orgid)\n" +
            "  left join ref_country on (ref_country.country_id = spec.country_id and lang = 'EN')\n" +
            "\n" +
            "order by country, species"),

    raw_indicator10 (
            "SELECT\n" +
            "  iteration::text as iteration,\n" +
            "  c.iso as country_iso3,\n" +
            "  a.orgid::text as org_id,\n" +
            "  orgname_l as org_name,\n" +
            "  a.id::text as answer_id,\n" +
            "  coalesce(t2.answer_freetext, '0') :: REAL AS sites_total,\n" +
            "  coalesce(t1.answer_freetext, '0') :: REAL AS sites_with_management\n" +
            "FROM answer a\n" +
            "  left JOIN answer_detail t1 ON (t1.answerId = a.id)\n" +
            "  left JOIN answer_detail t2 ON (t2.answerId = a.id)\n" +
            "\n" +
            "  left JOIN ref_country c ON (c.country_id = a.country_id)\n" +
            "  left join ref_instab on (ref_instab.id = a.orgid)\n" +
            "WHERE a.approved = 1 AND t1.subquestionid = 1043 AND t2.subquestionid = 1042"
    ),

    raw_indicator14 (
            "WITH crops AS (\n" +
            "    SELECT\n" +
            "      c.answerid, c.subquestionid, crop_id,\n" +
            "      coalesce(crop_name, answer_freetext) AS crop_name_en,\n" +
            "      answer_freetext_local AS crop_name_local\n" +
            "    FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id::text AND lang = 'EN')\n" +
            "    WHERE c.subquestionid = 1058\n" +
            "),\n" +
            "answers AS (\n" +
            "      select answerid, subquestionid, lang,\n" +
            "        string_agg(sl.option_value,'; ') AS value\n" +
            "      FROM\n" +
            "        answer_detail s\n" +
            "        LEFT JOIN ref_enum_options sl ON (s.reference_id = sl.id::text AND lang = 'EN')\n" +
            "      WHERE s.subquestionid IN (1057, 1059, 1060, 1061, 1062, 1063, 1064)\n" +
            "      GROUP BY answerid, subquestionid, lang\n" +
            ")\n" +
            "SELECT\n" +
            "  a.iteration :: TEXT,\n" +
            "  c.iso                                      AS country_iso3,\n" +
            "  a.orgid::text                              AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  s.value                                    AS strategy,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  gd.value                                   AS gaps,\n" +
            "  ogdl.value                                 AS other_gaps_local,\n" +
            "  ogd.value                                  AS other_gaps,\n" +
            "  m.value                                    AS methods,\n" +
            "  oml.value                                  AS other_methods_local,\n" +
            "  om.value                                   AS other_methods\n" +
            "FROM answer a\n" +
            "  LEFT JOIN answers s ON (s.subquestionid = 1057 AND a.id = s.answerid)\n" +
            "  LEFT JOIN answers gd ON (gd.subquestionid = 1059 AND a.id = gd.answerid)\n" +
            "  LEFT JOIN answers ogdl ON (ogdl.subquestionid = 1060 AND a.id = ogdl.answerid)\n" +
            "  LEFT JOIN answers ogd ON (ogd.subquestionid = 1061 AND a.id = ogd.answerid)\n" +
            "  LEFT JOIN answers m ON (m.subquestionid = 1062 AND a.id = m.answerid)\n" +
            "  LEFT JOIN answers oml ON (oml.subquestionid = 1063 AND a.id = oml.answerid)\n" +
            "  LEFT JOIN answers om ON (om.subquestionid = 1064 AND a.id = om.answerid)\n" +
            "  LEFT JOIN ref_country c ON (a.country_id = c.country_id)\n" +
            "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
            "  LEFT JOIN crops ON (crops.answerId = a.id)\n" +
            "WHERE a.questionid = 11 AND a.approved = 1\n" +
            "ORDER BY a.iteration, c.iso, a.id"
    ),

    raw_indicator15 (
            "WITH crops AS (\n" +
                    "  SELECT\n" +
                    "    c.answerid,\n" +
                    "    c.subquestionid,\n" +
                    "    crop_id,\n" +
                    "    coalesce(crop_name, answer_freetext) AS crop_name_en\n" +
                    "  FROM answer_detail c LEFT JOIN ref_crop rc ON (crop_id :: TEXT = c.reference_id AND lang = 'EN')\n" +
                    "  WHERE c.subquestionid = 1068\n" +
                    ")\n" +
                    "\n" +
                    "SELECT\n" +
                    "  co.iso                                                             AS country_iso,\n" +
                    "  it.WIEWS_INSTCODE                                                  AS stakeholder_code,\n" +
                    "  a.id::text                                                         AS answare_id,\n" +
                    "  a.questionid::text                                                 AS question_id,\n" +
                    "  a.orgid::text                                                      AS organization_id,\n" +
                    "  a.created_by,\n" +
                    "  a.created_date::text,\n" +
                    "  a.modified_by,\n" +
                    "  a.modified_date::text,\n" +
                    "  a.iteration::text,\n" +
                    "  a.datasource,\n" +
                    "  c.crop_name_en,\n" +
                    "  replace(f.answer_freetext, '\"', '''')                             AS answare,\n" +
                    "  t_start_date.answer_freetext                                       AS start_date,\n" +
                    "  coalesce(t_end_date.answer_freetext, t_start_date.answer_freetext) AS end_date\n" +
                    "FROM\n" +
                    "  (SELECT *\n" +
                    "   FROM answer\n" +
                    "   WHERE questionid = 12) a\n" +
                    "  LEFT JOIN answer_detail f ON (f.answerId = a.id AND f.subquestionId = 1065)\n" +
                    "  LEFT JOIN answer_detail t_start_date ON (t_start_date.answerId = a.id AND t_start_date.subquestionId = 1066)\n" +
                    "  LEFT JOIN answer_detail t_end_date ON (t_end_date.answerId = a.id AND t_end_date.subquestionId = 1067)\n" +
                    "  LEFT JOIN crops c ON (c.answerId = a.id)\n" +
                    "  LEFT JOIN ref_instab it ON (it.id = a.orgId)\n" +
                    "  LEFT JOIN ref_country co ON (co.country_id = a.country_id)\n" +
                    "WHERE a.approved = 1"
    ),

    raw_indicator16 ("SELECT  *\n" +
            "FROM\n" +
            "  ( SELECT\n" +
            "      a.iteration :: TEXT,\n" +
            "      a.id                     AS answer_ID,\n" +
            "      co.iso                   AS country,\n" +
            "      it.wiews_instcode        AS stakeholder,\n" +
            "      a.questionid :: TEXT  AS question_id,\n" +
            "      c.subquestionid :: TEXT  AS subquestion_id,\n" +
            "      CASE WHEN c.reference_id IS NOT NULL\n" +
            "        THEN crop_name\n" +
            "      ELSE answer_freetext END AS crop\n" +
            "    FROM\n" +
            "      answer a\n" +
            "      FULL OUTER JOIN\n" +
            "      answer_detail c\n" +
            "        ON ( c.answerid = a.id AND\n" +
            "             c.subquestionid IN ( 1068, 1070 ) )\n" +
            "      FULL OUTER JOIN\n" +
            "      ref_crop ref\n" +
            "        ON ( ref.crop_id :: TEXT = c.reference_id :: TEXT AND\n" +
            "             ref.lang = 'EN' )\n" +
            "      JOIN\n" +
            "      ref_instab it\n" +
            "        ON ( it.id = a.orgId )\n" +
            "      LEFT JOIN\n" +
            "      ref_country co\n" +
            "        ON ( co.country_id = a.country_id ) ) f\n" +
            "WHERE\n" +
            "  crop IS NOT NULL"),

    raw_indicator20 (
            "SELECT\n" +
            "  a.iteration::text as iteration,\n" +
            "  d.iso AS country_iso3,\n" +
            "  d.name::text as country,\n" +
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
            "  JOIN ref_instab c on a.orgId = c.id\n" +
            "  JOIN ref_country d ON (a.country_id = d.country_id and d.lang = 'EN')\n" +
            "  JOIN ref_country ref on (a.countryoriginid = ref.country_id and ref.lang = 'EN')\n" +
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
            "  a.orgid::text                              AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  coalesce(adn.answer_freetext, '0') :: REAL AS accessions_num,\n" +
            "  coalesce(adr.answer_freetext, '0') :: REAL AS accessions_regenerated,\n" +
            "  coalesce(adnr.answer_freetext, '0') :: REAL AS accessions_need_regeneration,\n" +
            "  coalesce(ado.answer_freetext, '0') :: REAL AS accessions_out_of_budget\n" +
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
            "  a.orgid::text                              AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  coalesce(adn.answer_freetext, '0') :: REAL AS accessions_num,\n" +
            "  coalesce(adr.answer_freetext, '0') :: REAL AS accessions_regenerated,\n" +
            "  coalesce(adnr.answer_freetext, '0') :: REAL AS accessions_need_regeneration,\n" +
            "  coalesce(ado.answer_freetext, '0') :: REAL AS accessions_out_of_budget\n" +
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
            "  a.orgid::text                              AS org_id,\n" +
            "  orgname_l                                  AS org_name,\n" +
            "  a.id                                       AS answer_id,\n" +
            "  crops.crop_id::text,\n" +
            "  crops.crop_name_en                         AS crop_name,\n" +
            "  crops.crop_name_local,\n" +
            "  coalesce(adg.answer_freetext, '0') :: REAL AS accessions_by_genebank,\n" +
            "  coalesce(adr.answer_freetext, '0') :: REAL AS accessions_by_research_centre,\n" +
            "  coalesce(adp.answer_freetext, '0') :: REAL AS accessions_by_private_sector,\n" +
            "  coalesce(adf.answer_freetext, '0') :: REAL AS accessions_by_farmer,\n" +
            "  coalesce(ado.answer_freetext, '0') :: REAL AS accessions_by_other_national,\n" +
            "  coalesce(ads.answer_freetext, '0') :: REAL AS accessions_by_stakeholder,\n" +
            "  coalesce(adu.answer_freetext, '0') :: REAL AS accessions_by_unknown,\n" +
            "  coalesce(sdg.answer_freetext, '0') :: REAL AS samples_by_genebank,\n" +
            "  coalesce(sdr.answer_freetext, '0') :: REAL AS samples_by_research_centre,\n" +
            "  coalesce(sdp.answer_freetext, '0') :: REAL AS samples_by_private_sector,\n" +
            "  coalesce(sdf.answer_freetext, '0') :: REAL AS samples_by_farmer,\n" +
            "  coalesce(sdo.answer_freetext, '0') :: REAL AS samples_by_other_national,\n" +
            "  coalesce(sds.answer_freetext, '0') :: REAL AS samples_by_stakeholder,\n" +
            "  coalesce(sdu.answer_freetext, '0') :: REAL AS samples_by_unknown\n" +
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
    )



    ;private String query;
    Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


