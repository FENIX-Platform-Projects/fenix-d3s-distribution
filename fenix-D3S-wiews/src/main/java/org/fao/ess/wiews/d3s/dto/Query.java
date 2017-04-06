package org.fao.ess.wiews.d3s.dto;

public enum Query {

    raw_indicator20 ("SELECT\n" +
            "\n" +
            "  cast(a.ITERATION as CHAR(50)) as iteration,\n" +
            "  cast(d.iso   as CHAR(50)) as country,\n" +
            "  cast(a.orgid  as CHAR(50)) as orgid,\n" +
            "  cast(c.wiews_instcode   as CHAR(50))as stakeholder,\n" +
            "  cast(a.id  as CHAR(50)) as id,\n" +
            "  cast(a.accessionno  as CHAR(50)) as accessionno,\n" +
            "  cast(a.taxonid  as CHAR(50)) as taxonid,\n" +
            "  cast(a.taxon_freetext  as CHAR(50)) as taxon_freetext,\n" +
            "  cast(a.cropid  as CHAR(50)) as cropid,\n" +
            "  cast(a.crop_freetext  as CHAR(50)) as crop_freetext,\n" +
            "  cast(a.acquisitiondate  as CHAR(50)) as acquisitiondate,\n" +
            "  cast(ref.iso  as CHAR(50)) as origincountry,\n" +
            "  cast(a.biologicalaccessionid  as CHAR(50)) as biologicalaccessionid,\n" +
            "  cast(a.genebankid  as CHAR(50)) as genebankid,\n" +
            "  cast(a.genebank_freetext  as CHAR(50)) as genebank_freetext,\n" +
            "  cast(a.latitude  as CHAR(50)) as latitude,\n" +
            "  cast(a.longitude  as CHAR(50)) as longitude,\n" +
            "  cast(a.collectionsourceid  as CHAR(50)) as collectionsourceid,\n" +
            "  cast(a.germplasmastoreid  as CHAR(50)) as germplasmastoreid,\n" +
            "  cast(a.multilateralsystemstatusid  as CHAR(50)) as multilateralsystemstatusid\n" +
            "\n" +
            "\n" +
            "FROM answer_q14 a\n" +
            "  JOIN\n" +
            "  ref_instab c\n" +
            "    on a.orgId = c.ID\n" +
            "  JOIN ref_country d ON a.country_id = d.country_id\n" +
            "  JOIN ref_country ref on a.countryoriginid = ref.country_id "),
    indicator20 ("SELECT\n" +
            "  cast(ITERATION as CHAR(50)) as iteration,\n" +
            "  '2240' as domain,\n" +
            "  'stk' as element,\n" +
            "  '20' as indicator,\n" +
            "  cast(a.biologicalAccessionId as CHAR(50)) as biologicalAccessionId,\n" +
            "  b.ISO as country,\n" +
            "  b.ISO as m49_country,\n" +
            "  c.WIEWS_INSTCODE as stakeholder,\n" +
            "  'na' as genus,\n" +
            "  count(distinct(SELECT IF (a.taxonId > 0, a.taxonId, a.taxon_freetext))) as value,\n" +
            "  '1' as um\n" +
            "FROM\n" +
            "  answer_q14 a\n" +
            "  JOIN\n" +
            "  ref_country b\n" +
            "    ON a.Country_ID = b.COUNTRY_ID\n" +
            "  JOIN\n" +
            "  ref_instab c\n" +
            "    on a.orgId = c.ID\n" +
            "WHERE\n" +
            "  a.approved = true\n" +
            "GROUP BY\n" +
            "  iteration,\n" +
            "  stakeholder,\n" +
            "  country\n" +
            "\n" +
            "UNION\n" +
            "/* By rating*/\n" +
            "\n" +
            "SELECT\n" +
            "  cast(iteration as CHAR(50)) as iteration,\n" +
            "  '2240' as domain,\n" +
            "  'nfp' as element,\n" +
            "  '20' as indicator,\n" +
            "  cast(null as CHAR(50)) as biologicalAccessionId,\n" +
            "  country,\n" +
            "  country as m49_country,\n" +
            "  'ZZZ' as stakeholder,\n" +
            "  'na' as genus,\n" +
            "  nfp_rating as VALUE,\n" +
            "  '%' as um\n" +
            "from\n" +
            "  (\n" +
            "    SELECT\n" +
            "      b.ISO as country,\n" +
            "      a.applicable,\n" +
            "      a.data_available,\n" +
            "      a.id,\n" +
            "      a.nfp_rating,\n" +
            "      a.iteration,\n" +
            "      a.comment\n" +
            "    FROM\n" +
            "      indicator_analysis a\n" +
            "      JOIN\n" +
            "      ref_country b\n" +
            "        ON (a.country_id = b.COUNTRY_ID)\n" +
            "    WHERE\n" +
            "      indicator_id = 20\n" +
            "  )\n" +
            "  t\n" +
            "GROUP BY\n" +
            "  iteration,\n" +
            "  domain,\n" +
            "  element,\n" +
            "  country\n" +
            "\n" +
            "union\n" +
            "/* By country*/\n" +
            "\n" +
            "SELECT\n" +
            "  iteration,\n" +
            "  '2240' as domain,\n" +
            "  'ind' as element,\n" +
            "  '20' as indicator,\n" +
            "  cast(null as CHAR(50)) as biologicalAccessionId,\n" +
            "  country,\n" +
            "   m49_country,\n" +
            "  'ZZZ' as stakeholder,\n" +
            "  'na' as genus,\n" +
            "  sum(value) as VALUE,\n" +
            "  '1' as um\n" +
            "FROM\n" +
            "  (\n" +
            "    SELECT\n" +
            "      cast(ITERATION as CHAR(50)) as iteration,\n" +
            "      '2240' as domain,\n" +
            "      'ind' as element,\n" +
            "      '20' as indicator,\n" +
            "      cast(a.biologicalAccessionId as CHAR(50)) as biologicalAccessionId,\n" +
            "      b.ISO as country,\n" +
            "      b.ISO as m49_country,\n" +
            "      'ZZZ' as stakeholder,\n" +
            "      'na' as genus,\n" +
            "      count(distinct(SELECT IF (a.taxonId > 0, a.taxonId, a.taxon_freetext))) as value,\n" +
            "      '1' as um\n" +
            "    FROM\n" +
            "      answer_q14 a\n" +
            "      JOIN\n" +
            "      ref_country b\n" +
            "        ON a.Country_ID = b.COUNTRY_ID\n" +
            "      JOIN\n" +
            "      ref_instab c\n" +
            "        on a.orgId = c.ID\n" +
            "    WHERE\n" +
            "      a.approved = true\n" +
            "      AND c.WIEWS_INSTCODE not in\n" +
            "          (\n" +
            "            'BEL084',\n" +
            "            'CIV033',\n" +
            "            'COL003',\n" +
            "            'ETH013',\n" +
            "            'IND002',\n" +
            "            'KEN056',\n" +
            "            'MEX002',\n" +
            "            'NGA039',\n" +
            "            'PER001',\n" +
            "            'PHL001',\n" +
            "            'SYR002',\n" +
            "            'TWN001',\n" +
            "            'FJI049',\n" +
            "            'SWE054'\n" +
            "          )\n" +
            "    GROUP BY\n" +
            "      iteration,\n" +
            "      stakeholder,\n" +
            "      country\n" +
            "  )\n" +
            "  z\n" +
            "GROUP BY\n" +
            "  iteration,\n" +
            "  domain,\n" +
            "  element,\n" +
            "  country\n" +
            "\n" +
            "UNION\n" +
            " /* By genus*/\n" +
            "SELECT\n" +
            "  cast(ITERATION as CHAR(50)) as iteration,\n" +
            "  '2240' as domain,\n" +
            "  'gen' as element,\n" +
            "  '20' as indicator,\n" +
            "  cast(a.biologicalAccessionId as CHAR(50)) as biologicalAccessionId,\n" +
            "  b.ISO as country,\n" +
            "  b.ISO as  m49_country,\n" +
            "  d.WIEWS_INSTCODE as stakeholder,\n" +
            "  lower(COALESCE(c.GENUS,'NA')) as genus,\n" +
            "  count(distinct(\n" +
            "    SELECT\n" +
            "      IF (a.taxonId > 0, a.taxonId, a.taxon_freetext))) as value,\n" +
            "  '1' as um\n" +
            "FROM\n" +
            "  answer_q14 a\n" +
            "  JOIN\n" +
            "  ref_country b\n" +
            "    ON a.Country_ID = b.COUNTRY_ID\n" +
            "  JOIN\n" +
            "  ref_taxtab c\n" +
            "    on a.taxonId = c.TAXON_ID\n" +
            "  JOIN\n" +
            "  ref_instab d\n" +
            "    on d.ID = a.orgId\n" +
            "WHERE\n" +
            "  a.approved = true\n" +
            "GROUP BY\n" +
            "  iteration,\n" +
            "  stakeholder,\n" +
            "  country,\n" +
            "  genus" ),
    indicator3("with raw as (\n" +
            " select * \n" +
            " from answer a\n" +
            " join answer_detail ad\n" +
            " on (a.id = answerid)\n" +
            " where iteration = 1 and questionid = 2\n" +
            "),\n" +
            "answer_1_2 as (\n" +
            " select iteration, iso as country_iso3, species, threatened, varieties, threatened_varieties, spec.answerid as answer_id\n" +
            " from\n" +
            " (select answerid, answer_freetext as species, iteration, country_id from raw where subquestionid = 1003) spec\n" +
            " join\n" +
            " (select answerid, case when reference_id = '1001' then true else false end as threatened from raw where subquestionid = 1004) tspec\n" +
            " on (spec.answerid = tspec.answerid)\n" +
            " join\n" +
            " (select answerid, answer_freetext::int as varieties from raw where subquestionid = 1005) var\n" +
            " on (spec.answerid = var.answerid)\n" +
            " join\n" +
            " (select answerid, answer_freetext::int as threatened_varieties from raw where subquestionid = 1006) tvar\n" +
            " on (spec.answerid = tvar.answerid)\n" +
            "\n" +
            " join ref_country on (ref_country.country_id = spec.country_id)\n" +
            "\n" +
            " order by country_iso3, species\n" +
            ")\n" +
            "\n" +
            "select '3_1' as indicator, iteration, country_iso3, count(*) as species_number, sum (threatened_inc) as threatened_species_number, (sum (threatened_inc)::real / count(*)) * 100 as threatened_percentage, 'perc' as um from\n" +
            "(select *, case when threatened then 1 else 0 end as threatened_inc from answer_1_2) raw\n" +
            "group by iteration, country_iso3\n" +
            "union\n" +
            "select '3_2' as indicator, iteration, country_iso3, sum (varieties) as varieties_number, sum (threatened_varieties) as threatened_varieties_number, (sum (threatened_varieties)::real / sum (varieties)) * 100 as threatened_percentage, 'perc' as um from\n" +
            "(select *, case when threatened then 1 else 0 end as threatened_inc from answer_1_2) raw\n" +
            "group by iteration, country_iso3\n" +
            "having sum (varieties)>0\n" +
            "\n" +
            "order by indicator, iteration, country_iso3"),
    raw_indicator3 ("with raw as (\n" +
            " select * \n" +
            " from answer a\n" +
            " join answer_detail ad\n" +
            " on (a.id = answerid)\n" +
            " where iteration = 1 and questionid = 2\n" +
            ")\n" +
            "\n" +
            "select iteration, iso as country_iso3, species, threatened, varieties, threatened_varieties, spec.answerid as answer_id\n" +
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
            "order by country_iso3, species\n");

    private String query;
    Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


