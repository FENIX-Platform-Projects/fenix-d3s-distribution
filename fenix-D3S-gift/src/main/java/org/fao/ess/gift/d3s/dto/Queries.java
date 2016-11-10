package org.fao.ess.gift.d3s.dto;

public enum Queries {
    loadSubgroupDailySubjectAvg(
        "select subject, round, group_code, subgroup_code, item, sum(value)/<<subjects>> as value, um from\n" +
        "(\n" +
        "\tselect s.subject, s.round, survey_day, group_code, subgroup_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject and s.round = c.round)\n" +
        "\tgroup by s.subject, s.round, survey_day, group_code, subgroup_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, s.round, survey_day, group_code, subgroup_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject and s.round = c.round)\n" +
        "\tgroup by s.subject, s.round, survey_day, group_code, subgroup_code\n" +
        ") data_by_day\n" +
        "group by  subject, round, group_code, subgroup_code, item, um\n"
    ),

    countSubjectRound("select count(*) as count from (select subject, round from <<tableName>> group by subject, round) subjects"),
    countSubject("select count(*) as count from (select subject from <<tableName>> group by subject) subjects"),
    loadFoodDailySubjectAvg(
        "with subjects_days_count AS ( select subject, count(*) as days_number from\n" +
        "( select subject, survey_day from consumption where survey_code = ? group by subject, survey_day ) subjects_days\n" +
        "group by subject )\n" +
        "select subjects_days_count.subject as subject, group_code, subgroup_code, foodex2_code, item, value/days_number as value, um from\n" +
        "(\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(food_amount_proc) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ENERGY'::varchar as item, sum(energy) as value, 'kcal'::varchar as um\n" +
        "  from consumption where survey_code = ? group by subject, foodex2_code\n" +
        ") consumption_total\n" +
        "join\n" +
        "subjects_days_count\n" +
        "on (consumption_total.subject = subjects_days_count.subject)\n"
    ),
    loadFoodDailySubjectRoundAvg(
        "with subjects_days_count AS ( select round, subject, count(*) as days_number from\n" +
        "( select round, subject, survey_day from consumption where survey_code = ? group by round, survey_day, subject ) subjects_days\n" +
        "group by round, subject )\n" +
        "select subjects_days_count.subject as subject, subjects_days_count.round as round, group_code, subgroup_code, foodex2_code, item, value/days_number as value, um from\n" +
        "(\n" +
        "  select round, subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(food_amount_proc) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? group by round, subject, foodex2_code\n" +
        "  union all\n" +
        "  select round, subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ENERGY'::varchar as item, sum(energy) as value, 'kcal'::varchar as um\n" +
        "  from consumption where survey_code = ? group by round, subject, foodex2_code\n" +
        ") consumption_total\n" +
        "join\n" +
        "subjects_days_count\n" +
        "on (consumption_total.round = subjects_days_count.round and consumption_total.subject = subjects_days_count.subject)\n"
    )
/*    loadFoodDailySubjectRoundAvg(
        "select subject, round, group_code, subgroup_code, foodex2_code, item, sum(value) as value, um from\n" +
        "(\n" +
        "\tselect s.subject, s.round, survey_day, group_code, subgroup_code, foodex2_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject and s.round = c.round)\n" +
        "\tgroup by s.subject, s.round, survey_day, group_code, subgroup_code, foodex2_code\n" +
        "\tunion all\n" +
        "\tselect s.subject, s.round, survey_day, group_code, subgroup_code, foodex2_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "\tfrom subject s join consumption c on (s.survey_code = ? and c.survey_code = ? and s.subject = c.subject and s.round = c.round)\n" +
        "\tgroup by s.subject, s.round, survey_day, group_code, subgroup_code, foodex2_code\n" +
        ") data_by_day\n" +
        "group by  subject, round, group_code, subgroup_code, foodex2_code, item, um\n"
    ),
*/
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