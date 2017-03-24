package org.fao.ess.wiews.d3s.dto;

public enum Queries {



    //CURRENT

    countSubjectRound("select count(*) as count from (select subject, round from subject where survey_code = ? group by subject, round) subjects"),
    countSubject("select count(*) as count from (select subject from subject where survey_code = ? group by subject) subjects"),
    loadFoodDailyTotalSubject(
        "with subjects AS (select subject, round, age_year, age_month, special_condition, gender from subject where survey_code = ? and round = 1 group by subject, round, age_year, age_month, special_condition, gender)\n"+
        "select consumption_total.item, consumption_total.subject as subject, consumption_total.round as round, survey_day, group_code, subgroup_code, foodex2_code, consumption_total.value, consumption_total.um, subjects.gender, subjects.special_condition, subjects.age_year, subjects.age_month, usa_hmd.value as suggested_value, 1::integer as increment from\n" +
        "(\n" +
        "  select subject, round, survey_day, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and FOOD_AMOUNT_PROC is not null group by subject, round, survey_day, foodex2_code\n" +
        ") consumption_total\n" +
        "join subjects on (consumption_total.subject = subjects.subject)\n"+
        "left join usa_hmd on (\n" +
        "  usa_hmd.item = consumption_total.item and \n" +
        "  usa_hmd.gender = subjects.gender and \n" +
        "  usa_hmd.special_condition = subjects.special_condition and \n" +
        "  usa_hmd.age_year_from <= subjects.age_year and \n" +
        "  usa_hmd.age_year_to > subjects.age_year \n" +
        "  )"
    ),
    loadFoodDailySubject(
        "with subjects_days_count AS ( select subject, count(*) as days_number from\n" +
        "( select subject, round, survey_day from consumption where survey_code = ? group by subject, round, survey_day ) subjects_days\n" +
        "group by subject ),\n" +
        "subjects AS (select subject, age_year, age_month, special_condition, gender from subject where survey_code = ? and round = 1 group by subject, age_year, age_month, special_condition, gender)\n"+
        "select consumption_total.item, consumption_total.subject as subject, group_code, subgroup_code, foodex2_code, consumption_total.value/days_number as value, consumption_total.um, subjects.gender, subjects.special_condition, subjects.age_year, subjects.age_month, usa_hmd.value as suggested_value, 1::integer as increment from\n" +
        "(\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and FOOD_AMOUNT_PROC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "  from consumption where survey_code = ? and ENERGY is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'PROTEIN'::varchar as item, sum(PROTEIN) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and PROTEIN is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'A_PROT'::varchar as item, sum(A_PROT) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and A_PROT is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'V_PROT'::varchar as item, sum(V_PROT) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and V_PROT is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CARBOH'::varchar as item, sum(CARBOH) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and CARBOH is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FAT'::varchar as item, sum(FAT) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and FAT is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'SAT_FAT'::varchar as item, sum(SAT_FAT) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and SAT_FAT is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CALC'::varchar as item, sum(CALC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and CALC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'IRON'::varchar as item, sum(IRON) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and IRON is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ZINC'::varchar as item, sum(ZINC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and ZINC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITC'::varchar as item, sum(VITC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'THIA'::varchar as item, sum(THIA) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and THIA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'RIBO'::varchar as item, sum(RIBO) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and RIBO is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'NIAC'::varchar as item, sum(NIAC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and NIAC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITB6'::varchar as item, sum(VITB6) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITB6 is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOLA'::varchar as item, sum(FOLA) as value, 'microgdfe'::varchar as um\n" +
        "  from consumption where survey_code = ? and FOLA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITB12'::varchar as item, sum(VITB12) as value, 'microg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITB12 is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITA'::varchar as item, sum(VITA) as value, 'micrograe'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'BCAROT'::varchar as item, sum(BCAROT) as value, 'microg'::varchar as um\n" +
        "  from consumption where survey_code = ? and BCAROT is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FIBTG'::varchar as item, sum(FIBTG) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and FIBTG is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ASH'::varchar as item, sum(ASH) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and ASH is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'MG'::varchar as item, sum(MG) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and MG is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'PHOS'::varchar as item, sum(PHOS) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and PHOS is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'POTA'::varchar as item, sum(POTA) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and POTA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'NA'::varchar as item, sum(NA) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and NA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CU'::varchar as item, sum(CU) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and CU is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'RETOL'::varchar as item, sum(RETOL) as value, 'microg'::varchar as um\n" +
        "  from consumption where survey_code = ? and RETOL is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITD'::varchar as item, sum(VITD) as value, 'microg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITD is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITE'::varchar as item, sum(VITE) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITE is not null group by subject, foodex2_code\n" +
        ") consumption_total\n" +
        "join subjects_days_count on (consumption_total.subject = subjects_days_count.subject)\n"+
        "join subjects on (consumption_total.subject = subjects.subject)\n"+
        "left join usa_hmd on (\n" +
        "  usa_hmd.item = consumption_total.item and \n" +
        "  usa_hmd.gender = subjects.gender and \n" +
        "  usa_hmd.special_condition = subjects.special_condition and \n" +
        "  usa_hmd.age_year_from <= subjects.age_year and \n" +
        "  usa_hmd.age_year_to > subjects.age_year \n" +
        "  )"
    ),
    loadFoodDailySubjectWeighted(
        "with subjects_days_count AS ( select subject, count(*) as days_number from\n" +
        "( select subject, round, survey_day from consumption where survey_code = ? group by subject, round, survey_day ) subjects_days\n" +
        "group by subject ),\n" +
        "subjects AS (select subject, age_year, age_month, special_condition, gender from subject where survey_code = ? and round = 1 group by subject, age_year, age_month, special_condition, gender)\n"+
        "select consumption_total.item, consumption_total.subject as subject, group_code, subgroup_code, foodex2_code, (consumption_total.value/days_number)/<<subjects>> as value, consumption_total.um, subjects.gender, subjects.special_condition, subjects.age_year, subjects.age_month, usa_hmd.value as suggested_value, 1::integer as increment from\n" +
        "(\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and FOOD_AMOUNT_PROC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
        "  from consumption where survey_code = ? and ENERGY is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'PROTEIN'::varchar as item, sum(PROTEIN) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and PROTEIN is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CARBOH'::varchar as item, sum(CARBOH) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and CARBOH is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FAT'::varchar as item, sum(FAT) as value, 'g'::varchar as um\n" +
        "  from consumption where survey_code = ? and FAT is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CALC'::varchar as item, sum(CALC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and CALC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'IRON'::varchar as item, sum(IRON) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and IRON is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ZINC'::varchar as item, sum(ZINC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and ZINC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITC'::varchar as item, sum(VITC) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITC is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOLA'::varchar as item, sum(FOLA) as value, 'microgdfe'::varchar as um\n" +
        "  from consumption where survey_code = ? and FOLA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITA'::varchar as item, sum(VITA) as value, 'micrograe'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITA is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITD'::varchar as item, sum(VITD) as value, 'microg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITD is not null group by subject, foodex2_code\n" +
        "  union all\n" +
        "  select subject, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITE'::varchar as item, sum(VITE) as value, 'mg'::varchar as um\n" +
        "  from consumption where survey_code = ? and VITE is not null group by subject, foodex2_code\n" +
        ") consumption_total\n" +
        "join subjects_days_count on (consumption_total.subject = subjects_days_count.subject)\n"+
        "join subjects on (consumption_total.subject = subjects.subject)\n"+
        "left join usa_hmd on (\n" +
            "  usa_hmd.item = consumption_total.item and \n" +
            "  usa_hmd.gender = subjects.gender and \n" +
            "  usa_hmd.special_condition = subjects.special_condition and \n" +
            "  usa_hmd.age_year_from <= subjects.age_year and \n" +
            "  usa_hmd.age_year_to > subjects.age_year \n" +
            "  )"
    ),



    //FUTURE

    loadFoodSubject(
            "select item, subjects.subject as subject, subjects.round as round, group_code, subgroup_code, foodex2_code, value, um, gender, special_condition, age_year, age_month from\n" +
                    "(\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'PROTEIN'::varchar as item, sum(PROTEIN) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'A_PROT'::varchar as item, sum(A_PROT) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'V_PROT'::varchar as item, sum(V_PROT) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CARBOH'::varchar as item, sum(CARBOH) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FAT'::varchar as item, sum(FAT) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'SAT_FAT'::varchar as item, sum(SAT_FAT) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CALC'::varchar as item, sum(CALC) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'IRON'::varchar as item, sum(IRON) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ZINC'::varchar as item, sum(ZINC) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITC'::varchar as item, sum(VITC) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'THIA'::varchar as item, sum(THIA) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'RIBO'::varchar as item, sum(RIBO) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'NIAC'::varchar as item, sum(NIAC) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITB6'::varchar as item, sum(VITB6) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FOLA'::varchar as item, sum(FOLA) as value, 'microgdfe'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITB12'::varchar as item, sum(VITB12) as value, 'microg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITA'::varchar as item, sum(VITA) as value, 'micrograe'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'BCAROT'::varchar as item, sum(BCAROT) as value, 'microg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'FIBTG'::varchar as item, sum(FIBTG) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'ASH'::varchar as item, sum(ASH) as value, 'g'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'MG'::varchar as item, sum(MG) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'PHOS'::varchar as item, sum(PHOS) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'POTA'::varchar as item, sum(POTA) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'NA'::varchar as item, sum(NA) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'CU'::varchar as item, sum(CU) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'RETOL'::varchar as item, sum(RETOL) as value, 'microg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITD'::varchar as item, sum(VITD) as value, 'microg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    "  union all\n" +
                    "  select subject, round, foodex2_code, max(subgroup_code) as subgroup_code, max(group_code) as group_code, 'VITE'::varchar as item, sum(VITE) as value, 'mg'::varchar as um\n" +
                    "  from consumption where survey_code = ? group by subject, round, foodex2_code\n" +
                    ") consumption_total\n"+
                    "join\n" +
                    "(select subject, round, gender, special_condition, age_year, age_month from subject where survey_code = ? group by subject, round, gender, special_condition, age_year, age_month) subjects\n" +
                    "on (consumption_total.subject = subjects.subject and consumption_total.round = subjects.round)\n"
    ),
    loadFoodDailySubjectRound(
        "with subjects_days_count AS ( select round, subject, count(*) as days_number from\n" +
        "( select round, subject, survey_day from consumption where survey_code = ? group by round, survey_day, subject ) subjects_days\n" +
        "group by round, subject )\n" +
        "select item, subjects_days_count.subject as subject, subjects_days_count.round as round, group_code, subgroup_code, foodex2_code, value/days_number as value, um from\n" +
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
    ),
    loadFoodDailySubjectRoundWeighted(
        "with subjects_days_count AS ( select round, subject, count(*) as days_number from\n" +
        "( select round, subject, survey_day from consumption where survey_code = ? group by round, survey_day, subject ) subjects_days\n" +
        "group by round, subject )\n" +
        "select item, subjects_days_count.subject as subject, subjects_days_count.round as round, group_code, subgroup_code, foodex2_code, (value/days_number)/<<subjects>> as value, um from\n" +
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

    ;

    private String query;
    Queries(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}


