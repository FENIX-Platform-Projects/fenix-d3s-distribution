package org.fao.ess.gift.d3s.dto;

public enum Queries {
    countSurveySubjects("select count(*) as count from (select distinct subject from <<tableName>>) subjects"),
    loadSubgroupDailySubjectAvg(
        "select subject, group_code, subgroup_code, item, avg(value) as value, um from\n" +
        "(\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        ") data_by_day\n" +
        "group by  subject, group_code, subgroup_code, item, um\n"
    ),
    loadFoodDailySubjectAvg(
        "select subject, group_code, subgroup_code, foodex2_code, item, avg(value) as value, um from\n" +
        "(\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, foodex2_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code, foodex2_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, foodex2_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code, foodex2_code\n" +
        ") data_by_day\n" +
        "group by  subject, group_code, subgroup_code, foodex2_code, item, um\n"
    ),

    ;

    private String query;
    Queries(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}



/*
    QUERY WITH ALL OF THE INDICATORS (DO NOT DELETE)

    loadSubgroupDailySubjectAvg(
        "select subject, group_code, subgroup_code, item, avg(value) as value, um from\n" +
        "(\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'FOOD_AMOUNT_UNPROC'::varchar as item, sum(FOOD_AMOUNT_UNPROC) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'PROTEIN'::varchar as item, sum(PROTEIN) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'A_PROT'::varchar as item, sum(A_PROT) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'V_PROT'::varchar as item, sum(V_PROT) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'CARBOH'::varchar as item, sum(CARBOH) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'FAT'::varchar as item, sum(FAT) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'SAT_FAT'::varchar as item, sum(SAT_FAT) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'CALC'::varchar as item, sum(CALC) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'IRON'::varchar as item, sum(IRON) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'ZINC'::varchar as item, sum(ZINC) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'VITC'::varchar as item, sum(VITC) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'THIA'::varchar as item, sum(THIA) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'RIBO'::varchar as item, sum(RIBO) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'NIAC'::varchar as item, sum(NIAC) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'VITB6'::varchar as item, sum(VITB6) as value, 'mg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'FOLA'::varchar as item, sum(FOLA) as value, 'microgdfe'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'VITB12'::varchar as item, sum(VITB12) as value, 'microg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'VITA'::varchar as item, sum(VITA) as value, 'micrograe'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, survey_day, group_code, subgroup_code, 'BCAROT'::varchar as item, sum(BCAROT) as value, 'microg'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject)\n" +
        "\tgroup by s.subject, survey_day, group_code, subgroup_code\n" +
        ") data_by_day\n" +
        "group by  subject, group_code, subgroup_code, item, um\n"
    ),

 */