package org.fao.ess.wiews.d3s.dto;

public enum DatasetType {

    //current
    foodSubjectDailyTotal("GIFT_fc_"),
    foodSubjectTotal("GIFT_afc_"),
    foodSubjectTotalWeighted("GIFT_wafc_"),

    //future
    foodSubjectConsumption("gift_process_subject_daily_consumption_"),
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
