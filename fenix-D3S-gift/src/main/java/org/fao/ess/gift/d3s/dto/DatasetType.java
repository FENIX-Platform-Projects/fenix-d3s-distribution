package org.fao.ess.gift.d3s.dto;

public enum DatasetType {

    dailySubjectAvgBySubgroup("gift_process_daily_avg_"),
    subgroupSubjectTotal("gift_process_total_subgroup_consumption_"),

    foodSubjectTotal("gift_process_total_food_consumption_"),
    foodSubjectTotalWeighted("gift_process_total_weighted_food_consumption_"),
    foodSubjectRoundTotal("gift_process_total_round_food_consumption_"),
    foodSubjectRoundTotalWeighted("gift_process_total_round_weighted_food_consumption_"),
    ;

    private String prefix;
    DatasetType(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

}
