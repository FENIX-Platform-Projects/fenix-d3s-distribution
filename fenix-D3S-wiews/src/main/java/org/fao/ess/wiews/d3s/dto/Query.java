package org.fao.ess.wiews.d3s.dto;

public enum Query {


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
            "  genus" );

    private String query;
    Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


