package org.fao.ess.wiews.d3s.dto;

public enum Query {


    indicator20 ("SELECT\n" +
            "\n" +
            "  cast(ITERATION as CHAR(50)) as iteration,\n" +
            "\n" +
            "\n" +
            "       '2240' as domain,\n" +
            "       'stk' as element,\n" +
            "       '20' as indicator,\n" +
            "       cast(a.biologicalAccessionId as CHAR(50)) as biologicalAccessionId,\n" +
            "       b.ISO as country,\n" +
            "       (SELECT it.WIEWS_INSTCODE FROM ref_instab it WHERE  it.id = a.orgId) as stakeholder,\n" +
            "       count(distinct(SELECT IF (a.taxonId > 0, a.taxonId, a.taxon_freetext))) as value,\n" +
            "       '1' as um\n" +
            "FROM answer_q14 a\n" +
            "  JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID\n" +
            "WHERE a.approved = true\n" +
            "GROUP BY iteration, stakeholder, country\n" +
            "\n" +
            "UNION\n" +
            "\n" +
            "\n" +
            "\n" +
            "SELECT\n" +
            "  cast(iteration as CHAR(50)) as iteration,\n" +
            "  '2240' as domain,\n" +
            "  'nfp' as element,\n" +
            "  '20' as indicator,\n" +
            "  cast(null as CHAR(50)) as biologicalAccessionId,\n" +
            "  country,\n" +
            "  'ZZZ' as stakeholder,\n" +
            "  nfp_rating as VALUE ,\n" +
            "  '%' as um\n" +
            "from ( SELECT\n" +
            "         b.ISO as country,\n" +
            "         a.applicable,\n" +
            "         a.data_available,\n" +
            "         a.id,\n" +
            "         a.nfp_rating,\n" +
            "         a.iteration,\n" +
            "         a.comment\n" +
            "       FROM indicator_analysis a\n" +
            "         JOIN ref_country b\n" +
            "           ON (a.country_id = b.COUNTRY_ID)\n" +
            "       WHERE indicator_id = 20)t\n" +
            "GROUP BY iteration, domain, element, country\n" +
            "\n" +
            "\n" +
            "union\n" +
            "\n" +
            "SELECT\n" +
            "  cast(ITERATION as CHAR(50)) as iteration,\n" +
            "\n" +
            "  '2240' as domain,\n" +
            "  'ind' as element,\n" +
            "  '20' as indicator,\n" +
            "  cast(null as CHAR(50)) as biologicalAccessionId,\n" +
            "  b.ISO as country,\n" +
            "  'ZZZ' as stakeholder,\n" +
            "\n" +
            "  count(*) as value,\n" +
            "  '1' as um\n" +
            "\n" +
            "FROM answer_q14 a\n" +
            "  JOIN ref_country b ON a.Country_ID = b.COUNTRY_ID\n" +
            "GROUP BY iteration,country\n" );

    private String query;
    Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


