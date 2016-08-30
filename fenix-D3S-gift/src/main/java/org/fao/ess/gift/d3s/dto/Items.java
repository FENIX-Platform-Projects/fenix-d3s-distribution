package org.fao.ess.gift.d3s.dto;

public enum Items {
    FOOD_AMOUNT_UNPROC("g"),
    FOOD_AMOUNT_PROC("g"),
    ENERGY("kcal"),
    PROTEIN("g"),
    A_PROT("g"),
    V_PROT("g"),
    CARBOH("g"),
    FAT("g"),
    SAT_FAT("g"),
    CALC("mg"),
    IRON("mg"),
    ZINC("mg"),
    VITC("mg"),
    THIA("mg"),
    RIBO("mg"),
    NIAC("mg"),
    VITB6("mg"),
    FOLA("microgdfe"),
    VITB12("microg"),
    VITA("micrograe"),
    BCAROT("microg");
    
    String um;

    Items(String um) {
        this.um = um;
    }

    public String getUm() {
        return um;
    }
}
