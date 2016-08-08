package org.fao.policy.d3s;

public enum Query {
    OECD_View_QueryDownloadPreview
            (
                    "select \n" +
                    "metadata_id,\n" +
                    "to_char(policy_id, 'FM999999999999999999') as policy_id,\n" +
                    "to_char(mastertable.cpl_id, 'FM999999999999999999') as cpl_id,\n" +
                    "to_char(commlistwithid.commodity_id, 'FM999999999999999999') as commodity_id,\n" +
                    "hs_version,\n" +
                    "hs_code,\n" +
                    "hs_suffix,\n" +
                    "policy_element,\n" +
                    "to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                    "to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date,\n" +
                    "units,\n" +
                    "value,\n" +
                    "value_text,\n" +
                    "value_type,\n" +
                    "exemptions,\n" +
                    "minavtariffvalue,\n" +
                    "notes,\n" +
                    "link,\n" +
                    "source,\n" +
                    "title_of_notice,\n" +
                    "legal_basis_name,\n" +
                    "to_number(to_char(date_of_publication,'YYYYMMDD'), '99999999') as date_of_publication,\n" +
                    "imposed_end_date,\n" +
                    "second_generation_specific,\n" +
                    "benchmark_tax,\n" +
                    "benchmark_product,\n" +
                    "tax_rate_biofuel,\n" +
                    "tax_rate_benchmark,\n" +
                    "start_date_tax,\n" +
                    "benchmark_link,\n" +
                    "original_dataset,\n" +
                    "to_char(type_of_change_code, 'FM999999999999999999') as type_of_change_code,\n" +
                    "type_of_change_name,\n" +
                    "measure_description,\n" +
                    "product_original_hs,\n" +
                    "product_original_name,\n" +
                    "link_pdf,\n" +
                    "benchmark_link_pdf,\n" +
                    "short_description,\n" +
                    "shared_group_code,\n" +
                    "description,\n" +
                    "maxavtariffvalue,\n" +
                    "countavtariff,\n" +
                    "countnavtariff\n" +
                    "from mastertable, policytable, commlistwithid where mastertable.cpl_id = policytable.cpl_id and commlistwithid.commodity_id = policytable.commodity_id;\n"
            ),
    OECD_View_QueryDownload
            (
                    "select \n" +
                    "to_char(mastertable.cpl_id, 'FM999999999999999999') as cpl_id,\n" +
                    "to_char(policytable.commodity_id, 'FM999999999999999999') as commodity_id,\n" +
                    "to_char(mastertable.country_code, 'FM999999999999999999') as country_code,\n" +
                    "to_char(mastertableB.subnational_code, 'FM999999999999999999') as subnational_code,\n" +
                    "to_char(mastertable.commoditydomain_code, 'FM999999999999999999') as commoditydomain_code,\n" +
                    "to_char(mastertable.commodityclass_code, 'FM999999999999999999') as commodityclass_code,\n" +
                    "to_char(mastertable.policydomain_code, 'FM999999999999999999') as policydomain_code,\n" +
                    "to_char(mastertable.policytype_code, 'FM999999999999999999') as policytype_code,\n" +
                    "to_char(mastertable.policymeasure_code, 'FM999999999999999999') as policymeasure_code,\n" +
                    "to_char(mastertable.condition_code, 'FM999999999999999999') as condition_code,\n" +
                    "to_char(mastertable.individualpolicy_code, 'FM999999999999999999') as individualpolicy_code,\n" +
                    "to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                    "to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date\n" +
                    "from mastertable, mastertableB, policytable where mastertable.cpl_id = policytable.cpl_id and mastertable.cpl_id = mastertableB.cpl_id\n"
            ),
    OECD_View_ImportTariffs
            (
                    "select \n" +
                    "to_char(policydomain_code, 'FM999999999999999999') as policydomain_code,\n" +
                    "to_char(commoditydomain_code, 'FM999999999999999999') as commoditydomain_code,\n" +
                    "to_char(mastertable.policytype_code, 'FM999999999999999999') as policytype_code,\n" +
                    "to_char(mastertable.policymeasure_code, 'FM999999999999999999') as policymeasure_code,\n" +
                    "to_char(mastertable.commodityclass_code, 'FM999999999999999999') as commodityclass_code,\n" +
                    "to_char(mastertable.country_code, 'FM999999999999999999') as country_code,\n" +
                    "commlistwithid.hs_code,\n" +
                    "SUBSTRING(commlistwithid.hs_code from 0 for 5) as hs_prefix,\n"+
                    "to_char(element_code, 'FM999999999999999999') as element_code,\n" +
                    "value,\n" +
                    "units,\n" +
                    "to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                    "to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date,\n" +
                    "to_number(to_char(start_date,'YYYY'), '9999') as beginning_year\n" +
                    "from mastertable, policytable, commlistwithid where mastertable.cpl_id = policytable.cpl_id AND commlistwithid.commodity_id = policyTableViewTest.commodity_id;\n"
            ),
    OECD_View_BiofuelPolicy
            (
                    "select \n" +
                    "to_char(policydomain_code, 'FM999999999999999999') as policydomain_code,\n" +
                    "to_char(commoditydomain_code, 'FM999999999999999999') as commoditydomain_code,\n" +
                    "to_char(mastertable.policytype_code, 'FM999999999999999999') as policytype_code,\n" +
                    "to_char(mastertable.policymeasure_code, 'FM999999999999999999') as policymeasure_code,\n" +
                    "to_char(mastertable.commodityclass_code, 'FM999999999999999999') as commodityclass_code,\n" +
                    "to_char(mastertable.country_code, 'FM999999999999999999') as country_code,\n"+
                    "to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                    "to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date,\n" +
                    "to_number(to_char(start_date,'YYYY'), '9999') as beginning_year\n" +
                    "from mastertable, policyTableViewTest where mastertable.cpl_id = policyTableViewTest.cpl_id;\n"
            );

    private String query;
    Query(String query) {
        this.query = query;
    }

    @Override
    public String toString() {
        return query;
    }
}
