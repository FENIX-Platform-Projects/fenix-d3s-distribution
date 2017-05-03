package org.fao.ess.wiews.d3s.dto;

public enum Query {

    indicator2 ("select * from indicators.indicator2" ),
    indicator3 ("select * from indicators.indicator3" ),
    indicator10 ("select * from indicators.indicator10" ),
    indicator14 ("select * from indicators.indicator14" ),

    indicator20 ("select * from indicators.indicator20" ),

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
            "SELECT   iteration,\n" +
            "  iso AS country_iso3,\n" +
            "  species,\n" +
            "  varieties,\n" +
            "  spec.answerid AS answer_id\n" +
            "FROM\n" +
            "  ( SELECT answerid,\n" +
            "      answer_freetext AS species,\n" +
            "      iteration,\n" +
            "      country_id\n" +
            "    FROM   raw\n" +
            "    WHERE  subquestionid = 1003 ) spec\n" +
            "  JOIN (\n" +
            "         SELECT\n" +
            "           answerid,\n" +
            "           answer_freetext :: int AS varieties\n" +
            "         FROM   raw\n" +
            "         WHERE  subquestionid = 1005 ) var\n" +
            "    ON (spec.answerid = var.answerid )\n" +
            "  JOIN ref_country ON ( ref_country.country_id = spec.country_id )\n" +
            "ORDER BY\n" +
            "  country_iso3,\n" +
            "  species"),

    raw_indicator3 ("with raw as (\n" +
            " select * \n" +
            " from answer a\n" +
            " join answer_detail ad\n" +
            " on (a.id = answerid)\n" +
            " where iteration = 1 and questionid = 2\n" +
            ")\n" +
            "\n" +
            "select iteration::text, iso as country_iso3, species, threatened, varieties, threatened_varieties, spec.answerid as answer_id\n" +
            "from\n" +
            "(select answerid, answer_freetext as species, iteration, country_id from raw where subquestionid = 1003) spec\n" +
            "join\n" +
            "(select answerid, case when reference_id = '1001' then true else false end as threatened from raw where subquestionid = 1004) tspec\n" +
            "on (spec.answerid = tspec.answerid)\n" +
            "join\n" +
            "(select answerid, answer_freetext::int as varieties from raw where subquestionid = 1005) var\n" +
            "on (spec.answerid = var.answerid)\n" +
            "join\n" +
            "(select answerid, answer_freetext::int as threatened_varieties from raw where subquestionid = 1006) tvar\n" +
            "on (spec.answerid = tvar.answerid)\n" +
            "\n" +
            "join ref_country on (ref_country.country_id = spec.country_id)\n" +
            "\n" +
            "order by country_iso3, species\n"),

    raw_indicator10 (
            "SELECT\n" +
            "  c.iso as country_iso3,\n" +
            "  a.id::text as answer_id,\n" +
            "  questionid::text as question_id,\n" +
            "  a.orgid::text as organization_id,\n" +
            "  case when approved=1 then TRUE ELSE FALSE end as approved,\n" +
            "  created_by,\n" +
            "  created_date::text as created_date,\n" +
            "  modified_by,\n" +
            "  modified_date::text as modified_date,\n" +
            "  iteration::text as iteration,\n" +
            "  datasource,\n" +
            "  coalesce(t1.answer_freetext, '0') :: REAL AS sites_with_management,\n" +
            "  coalesce(t2.answer_freetext, '0') :: REAL AS sites_total\n" +
            "FROM answer a\n" +
            "  JOIN answer_detail t1 ON (t1.answerId = a.id)\n" +
            "  JOIN answer_detail t2 ON (t2.answerId = a.id)\n" +
            "  JOIN ref_country c ON (c.country_id = a.country_id)\n" +
            "WHERE a.approved = 1 AND t1.subquestionid = 1043 AND t2.subquestionid = 1042"
    ),

    raw_indicator20 ("SELECT\n" +
            "\n" +
            "  a.ITERATION::text as iteration,\n" +
            "  d.iso::text as country,\n" +
            "  a.orgid::text as orgid,\n" +
            "  c.wiews_instcode::text as stakeholder,\n" +
            "  a.id::text as id,\n" +
            "  a.accessionno::text as accessionno,\n" +
            "  a.taxonid::text as taxonid,\n" +
            "  a.taxon_freetext::text as taxon_freetext,\n" +
            "  a.cropid::text as cropid,\n" +
            "  a.crop_freetext::text as crop_freetext,\n" +
            "  a.acquisitiondate::text as acquisitiondate,\n" +
            "  ref.iso::text as origincountry,\n" +
            "  a.biologicalaccessionid::text as biologicalaccessionid,\n" +
            "  a.genebankid::text as genebankid,\n" +
            "  a.genebank_freetext::text as genebank_freetext,\n" +
            "  a.latitude::text as latitude,\n" +
            "  a.longitude::text as longitude,\n" +
            "  a.collectionsourceid::text as collectionsourceid,\n" +
            "  a.germplasmastoreid::text as germplasmastoreid,\n" +
            "  a.multilateralsystemstatusid::text as multilateralsystemstatusid\n" +
            "\n" +
            "\n" +
            "FROM answer_q14 a\n" +
            "  JOIN\n" +
            "  ref_instab c\n" +
            "    on a.orgId = c.ID\n" +
            "  JOIN ref_country d ON a.country_id = d.country_id\n" +
            "  JOIN ref_country ref on a.countryoriginid = ref.country_id "),



    ;private String query;
    Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


