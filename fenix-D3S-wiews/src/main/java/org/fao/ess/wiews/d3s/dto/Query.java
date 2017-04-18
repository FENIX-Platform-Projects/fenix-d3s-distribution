package org.fao.ess.wiews.d3s.dto;

public enum Query {

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
    indicator20 ("select * from indicator20" ),
    indicator3("WITH raw AS (\n" +
            "    SELECT\n" +
            "      *\n" +
            "    FROM\n" +
            "      answer a\n" +
            "      JOIN\n" +
            "      answer_detail ad\n" +
            "        ON ( a.id = answerid )\n" +
            "    WHERE\n" +
            "      iteration = 1 AND questionid = 2\n" +
            "), answer_1_2 AS (\n" +
            "    SELECT\n" +
            "      iteration,\n" +
            "      iso           AS country_iso3,\n" +
            "      species,\n" +
            "      threatened,\n" +
            "      varieties,\n" +
            "      threatened_varieties,\n" +
            "      spec.answerid AS answer_id\n" +
            "    FROM\n" +
            "      ( SELECT\n" +
            "          answerid,\n" +
            "          answer_freetext AS species,\n" +
            "          iteration,\n" +
            "          country_id\n" +
            "        FROM\n" +
            "          raw\n" +
            "        WHERE\n" +
            "          subquestionid = 1003 ) spec\n" +
            "      JOIN\n" +
            "      ( SELECT\n" +
            "          answerid,\n" +
            "          CASE WHEN reference_id = '1001'\n" +
            "            THEN TRUE\n" +
            "          ELSE FALSE END AS threatened\n" +
            "        FROM\n" +
            "          raw\n" +
            "        WHERE\n" +
            "          subquestionid = 1004 ) tspec\n" +
            "        ON ( spec.answerid = tspec.answerid )\n" +
            "      JOIN\n" +
            "      ( SELECT\n" +
            "          answerid,\n" +
            "          answer_freetext :: INT AS varieties\n" +
            "        FROM\n" +
            "          raw\n" +
            "        WHERE\n" +
            "          subquestionid = 1005 ) var\n" +
            "        ON ( spec.answerid = var.answerid )\n" +
            "      JOIN\n" +
            "      ( SELECT\n" +
            "          answerid,\n" +
            "          answer_freetext :: INT AS threatened_varieties\n" +
            "        FROM\n" +
            "          raw\n" +
            "        WHERE\n" +
            "          subquestionid = 1006 ) tvar\n" +
            "        ON ( spec.answerid = tvar.answerid )\n" +
            "      JOIN\n" +
            "      ref_country\n" +
            "        ON ( ref_country.country_id = spec.country_id )\n" +
            "    ORDER BY\n" +
            "      country_iso3,\n" +
            "      species\n" +
            "), total AS (\n" +
            "\n" +
            "  SELECT\n" +
            "    '3_1'                                              AS indicator,\n" +
            "    iteration,\n" +
            "    country_iso3,\n" +
            "    count (*)                                          AS species_number,\n" +
            "    sum (threatened_inc)                               AS threatened_species_number,\n" +
            "    ( sum (threatened_inc) :: REAL / count (*) ) * 100 AS threatened_percentage,\n" +
            "    '%'                                                AS um\n" +
            "  FROM\n" +
            "    ( SELECT\n" +
            "        *,\n" +
            "        CASE WHEN threatened\n" +
            "          THEN 1\n" +
            "        ELSE 0 END AS threatened_inc\n" +
            "      FROM\n" +
            "        answer_1_2 ) raw\n" +
            "  GROUP BY\n" +
            "    iteration,\n" +
            "    country_iso3\n" +
            "  UNION\n" +
            "  SELECT\n" +
            "    '3_2'                                                          AS indicator,\n" +
            "    iteration,\n" +
            "    country_iso3,\n" +
            "    sum (varieties)                                                AS varieties_number,\n" +
            "    sum (threatened_varieties)                                     AS threatened_varieties_number,\n" +
            "    ( sum (threatened_varieties) :: REAL / sum (varieties) ) * 100 AS threatened_percentage,\n" +
            "    '%'                                                            AS um\n" +
            "  FROM\n" +
            "    ( SELECT\n" +
            "        *,\n" +
            "        CASE WHEN threatened\n" +
            "          THEN 1\n" +
            "        ELSE 0 END AS threatened_inc\n" +
            "      FROM\n" +
            "        answer_1_2 ) raw\n" +
            "  GROUP BY\n" +
            "    iteration,\n" +
            "    country_iso3\n" +
            "  HAVING sum (varieties) > 0\n" +
            "  ORDER BY\n" +
            "    indicator,\n" +
            "    iteration,\n" +
            "    country_iso3)\n" +
            "\n" +
            "SELECT\n" +
            "  '1130' AS domain,\n" +
            "  indicator,\n" +
            "  iteration,\n" +
            "  'ind' as element,\n" +
            "  country_iso3,\n" +
            "  species_number,\n" +
            "  threatened_species_number,\n" +
            "  threatened_percentage,\n" +
            "  um\n" +
            "\n" +
            "FROM\n" +
            "  total\n" +
            "UNION\n" +
            "SELECT\n" +
            "  '1130'                      AS domain,\n" +
            "  cast (indicator_id AS TEXT) AS indicator,\n" +
            "  iteration,\n" +
            "  'nfp' as element,\n" +
            "\n" +
            "  cast (iso AS TEXT)          AS country,\n" +
            "  0                           AS sum,\n" +
            "  0                           AS threatened_sum,\n" +
            "  nfp_rating                  AS percentage,\n" +
            "  '%'                         AS um\n" +
            "FROM\n" +
            "  indicator_analysis spec\n" +
            "  JOIN\n" +
            "  ref_country\n" +
            "    ON ( ref_country.country_id = spec.country_id )\n" +
            "WHERE\n" +
            "  indicator_id = 3"),
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


