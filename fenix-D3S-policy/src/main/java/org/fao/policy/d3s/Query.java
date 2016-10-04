package org.fao.policy.d3s;

public enum Query {

    OECD_View_QueryDownload
            (
               "SELECT\n" +
                       "   to_char(cpl.cpl_id,\n" +
                       "   'FM999999999999999999') AS cpl_id,\n" +
                       "   to_char(policy.commodity_id,\n" +
                       "   'FM999999999999999999') AS commodity_id,\n" +
                       "   cpl.country_code,\n" +
                       "   cpl_subnational.subnational_code,\n" +
                       "   cpl.commoditydomain_code,\n" +
                       "   cpl.commodityclass_code,\n" +
                       "   cpl.policydomain_code,\n" +
                       "   cpl.policytype_code,\n" +
                       "   cpl.policymeasure_code,\n" +
                       "   cpl.condition_code,\n" +
                       "   cpl.individualpolicy_code,\n" +
                       "   to_number(to_char(start_date,\n" +
                       "   'YYYYMMDD'),\n" +
                       "   '99999999') AS start_date,\n" +
                       "   to_number(to_char(end_date,\n" +
                       "   'YYYYMMDD'),\n" +
                       "   '99999999') AS end_date \n" +
                       "FROM\n" +
                       "   cpl,\n" +
                       "   cpl_subnational,\n" +
                       "   policy  \n" +
                       "WHERE\n" +
                       "   cpl.cpl_id = policy.cpl_id  \n" +
                       "   AND cpl.cpl_id = cpl_subnational.cpl_id"
            ),
    OECD_View_ImportTariffs
            (
                "     select\n" +
                        "                    cpl.policydomain_code,\n" +
                        "                    cpl.commoditydomain_code,\n" +
                        "                    cpl.policytype_code,\n" +
                        "                    cpl.policymeasure_code,\n" +
                        "                    cpl.commodityclass_code,\n" +
                        "                    cpl.country_code,\n" +
                        "                    commodity.hs_code,\n" +
                        "                    SUBSTRING(commodity.hs_code from 0 for 5) as hs_prefix,\n" +
                        "                    element_code,\n" +
                        "                    value,\n" +
                        "                    units,\n" +
                        "                    to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                        "                    to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date,\n" +
                        "                    to_number(to_char(start_date,'YYYY'), '9999') as beginning_year,\n" +
                        "                    to_number(to_char(end_date,'YYYY'), '9999') as ending_year\n" +
                        "                    from cpl, policy, commodity where cpl.cpl_id = policy.cpl_id AND commodity.commodity_id = policy.commodity_id;\n"
            ),
    OECD_View_BiofuelPolicy
            (
                  "   select\n" +
                          "                    policydomain_code,\n" +
                          "                    commoditydomain_code,\n" +
                          "                    cpl.policytype_code,\n" +
                          "                    cpl.policymeasure_code,\n" +
                          "                    cpl.commodityclass_code,\n" +
                          "                    cpl.country_code,\n" +
                          "                    to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                          "                    to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date,\n" +
                          "                    to_number(to_char(start_date,'YYYY'), '9999') as beginning_year,\n" +
                          "                    to_number(to_char(end_date,'YYYY'), '9999') as ending_year\n" +
                          "                    from cpl, policy where cpl.cpl_id = policy.cpl_id;"
            ),
    
    OECD_Timeseries_BiofuelPolicy (
       "   with timeseries as (\n" +
               "                                     select to_number(to_char(generate_series('2011-01-01'::timestamp, '2014-12-31'::timestamp, '1 day'),\n" +
               "                                     'YYYYMMDD'),'99999999')\n" +
               "                                     as  day )\n" +
               "\n" +
               "                                     select\n" +
               "                                     final.commodityclass,\n" +
               "                                     final.policytype,\n" +
               "                                     final.policymeasure,\n" +
               "                                     final.country,\n" +
               "                                     final.day\n" +
               "\n" +
               "                                     from\n" +
               "                                     (select *\n" +
               "                                      from\n" +
               "                                      timeseries\n" +
               "                                     left join\n" +
               "                                     (select\n" +
               "                                     m.commodityclass_code as commodityclass,\n" +
               "                                     m.policytype_code as policytype,\n" +
               "                                     m.policymeasure_code as policymeasure,\n" +
               "                                     m.country_code as country,\n" +
               "                                     to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
               "                                     to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date\n" +
               "                                     from\n" +
               "                                     (select * from cpl where\n" +
               "                                     policytype_code in ('8','10','1','2','12','9')\n" +
               "                                     and commodityclass_code IN ('5','6','7'))m,\n" +
               "                                     policy,\n" +
               "                                     commodity\n" +
               "                                     where m.cpl_id = policy.cpl_id\n" +
               "                                     AND\n" +
               "                                     commodity.commodity_id = policy.commodity_id\n" +
               "\n" +
               "                                     group by\n" +
               "\n" +
               "                                     commodityclass,\n" +
               "                                     policytype,\n" +
               "                                     policymeasure,\n" +
               "                                     country,\n" +
               "                                     start_date,\n" +
               "                                     end_date\n" +
               "                                     order by\n" +
               "                                     commodityclass,\n" +
               "                                     policytype,\n" +
               "                                     policymeasure,\n" +
               "                                     country,\n" +
               "                                     start_date,\n" +
               "                                     end_date\n" +
               "                                     )h\n" +
               "                                     on\n" +
               "                                     ((h.end_date is null AND day >= h.start_date) OR\n" +
               "                                     (day  between h.start_date and h.end_date) ))  final\n" +
               "                                     group by\n" +
               "                                     final.commodityclass,\n" +
               "                                     final.policytype,\n" +
               "                                     final.policymeasure,\n" +
               "                                     final.country,\n" +
               "                                     final.day\n" +
               "                                     order by\n" +
               "                                     final.commodityclass,\n" +
               "                                     final.policytype,\n" +
               "                                     final.policymeasure,\n" +
               "                                     final.country,\n" +
               "                                     final.day"
    ),
    OECD_Timeseries_ExportRestrictions (
              "      with timeseries as (\n" +
                      "                                       select to_number(to_char(generate_series('2007-01-01'::timestamp, '2014-12-31'::timestamp, '1 day'),\n" +
                      "                                       'YYYYMMDD'),'99999999')\n" +
                      "                                       as  day )\n" +
                      "\n" +
                      "                                       select\n" +
                      "                                       final.commodityclass,\n" +
                      "                                       final.policytype,\n" +
                      "                                       final.policymeasure,\n" +
                      "                                       final.country,\n" +
                      "                                       final.day\n" +
                      "\n" +
                      "                                       from\n" +
                      "                                       (select *\n" +
                      "                                        from\n" +
                      "                                        timeseries\n" +
                      "                                        left join\n" +
                      "                                       (select\n" +
                      "                                        m.commodityclass_code as commodityclass,\n" +
                      "                                       m.policytype_code as policytype,\n" +
                      "                                       m.policymeasure_code as policymeasure,\n" +
                      "                                       m.country_code as country,\n" +
                      "                                       to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date,\n" +
                      "                                       to_number(to_char(end_date,'YYYYMMDD'), '99999999') as end_date\n" +
                      "                                       from\n" +
                      "                                       (select * from cpl where\n" +
                      "                                       policytype_code in ('1')\n" +
                      "                                       and commodityclass_code IN ('1','2','3','4')\n" +
                      "                                       and policymeasure_code IN('1','2','4','5','8'))m,\n" +
                      "                                       policy,\n" +
                      "                                       commodity\n" +
                      "                                       where m.cpl_id = policy.cpl_id\n" +
                      "                                       AND\n" +
                      "                                       commodity.commodity_id = policy.commodity_id\n" +
                      "\n" +
                      "                                       group by\n" +
                      "\n" +
                      "                                       commodityclass,\n" +
                      "                                       policytype,\n" +
                      "                                       policymeasure,\n" +
                      "                                       country,\n" +
                      "                                       start_date,\n" +
                      "                                       end_date\n" +
                      "                                       order by\n" +
                      "                                       commodityclass,\n" +
                      "                                       policytype,\n" +
                      "                                       policymeasure,\n" +
                      "                                       country,\n" +
                      "                                       start_date,\n" +
                      "                                       end_date\n" +
                      "                                       )h\n" +
                      "                                       on\n" +
                      "                                       ((h.end_date is null AND day >= h.start_date) OR\n" +
                      "                                       (day  between h.start_date and h.end_date) ))  final\n" +
                      "                                       group by\n" +
                      "                                       final.commodityclass,\n" +
                      "                                       final.policytype,\n" +
                      "                                       final.policymeasure,\n" +
                      "                                       final.country,\n" +
                      "                                       final.day\n" +
                      "                                       order by\n" +
                      "                                       final.commodityclass,\n" +
                      "                                       final.policytype,\n" +
                      "                                       final.policymeasure,\n" +
                      "                                       final.country,\n" +
                      "                                       final.day"
    ),
    OECD_Timeseries_ImportTariffs (
           " with timeseries as (\n" +
                   "                                          select to_number(to_char(generate_series('2012-01-01'::timestamp, '2015-12-31'::timestamp, '1 year'),\n" +
                   "                                          'YYYY'),'9999')\n" +
                   "                                          as  year )\n" +
                   "\n" +
                   "                                          select\n" +
                   "                                          year,\n" +
                   "                                          hs_tariff,\n" +
                   "                                          commodityclass,\n" +
                   "                                          case element_code\n" +
                   "                                          when '4' then '1'\n" +
                   "                                          when '8' then '2'\n" +
                   "                                          end as policyelement,\n" +
                   "                                          units,\n" +
                   "                                          value\n" +
                   "\n" +
                   "                                          from\n" +
                   "                                          timeseries\n" +
                   "                                          join\n" +
                   "                                           (select HS_Tariff,commodityclass, element_code, start_year, end_year, units, value from (\n" +
                   "                                          select  * from (\n" +
                   "                                                 select\n" +
                   "                                                 cpl.commodityclass_code as commodityclass,\n" +
                   "                                                 policy.element_code,\n" +
                   "                                                 SUBSTRING(commodity.HS_Code from 0 for 5) as hs_tariff,\n" +
                   "                                                 to_number(to_char(start_date,'YYYY'), '9999') as start_year,\n" +
                   "                                                 to_number(to_char(end_date,'YYYY'), '9999') as end_year,\n" +
                   "                                                 commodity.HS_Code,\n" +
                   "                                                 policy.units,\n" +
                   "                                                 policy.value\n" +
                   "                                                 from\n" +
                   "                                                 cpl  JOIN policy ON cpl.CPL_ID = policy.CPL_ID\n" +
                   "                                                  JOIN commodity ON cpl.Commodity_ID = commodity.Commodity_ID)t\n" +
                   "\n" +
                   "                                          WHERE hs_tariff IN ('1001','1005','1006','1201')\n" +
                   "                                          and t.element_code in ('4','8')\n" +
                   "                                          and t.units = '1'\n" +
                   "                                                  )g\n" +
                   "                                               )h\n" +
                   "\n" +
                   "                                           on ((h.end_year is null AND year >= h.start_year) OR\n" +
                   "                                          (year  between h.start_year and h.end_year) )"
    ),
    OECD_CommodityClassFilter (
            "select * from commodityclass"
    ),
    OECD_Policy_Master (
            "select cpl.cpl_id, cpl.country_code, subnational_codes, cpl.commodityclass_code, cpl.commodity_id, policydomain_code, policytype_code, policymeasure_code, condition_code, individualpolicy_code,\n" +
            "policy_id, metadata_id, to_number(to_char(start_date,'YYYYMMDD'), '99999999') as start_date, coalesce(to_number(to_char(end_date,'YYYYMMDD'), '99999999'),99991231) as end_date, value_text, element_code,\n" +
            "hs_code, hs_version,\n" +
            "\n" +
            "commoditydomain_code,\n" +
            "\n" +
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
            "to_number(to_char(start_date_tax,'YYYYMMDD'), '99999999') as start_date_tax,\n" +
            "benchmark_link,\n" +
            "original_dataset,\n" +
            "type_of_change_code,\n" +
            "measure_description,\n" +
            "product_original_hs,\n" +
            "product_original_name,\n" +
            "link_pdf,\n" +
            "benchmark_link_pdf,\n" +
            "maxavtariffvalue,\n" +
            "countavtariff,\n" +
            "countnavtariff,\n" +
            "units, \n" +
            "value, \n" +
            "value_type,\n" +
            "\n" +
            "hs_suffix,\n" +
            "description,\n" +
            "short_description,\n" +
            "sharedgroup_code\n" +
            "\n" +
            "from\n" +
            "(\n" +
            "\tselect cpl.*, subnational_codes\n" +
            "\tfrom cpl join\n" +
            "\t(select cpl_id, string_agg(subnational_code,',') as subnational_codes from cpl_subnational group by cpl_id) subnational\n" +
            "\ton (cpl.cpl_id = subnational.cpl_id)\n" +
            ") cpl join policy on (cpl.cpl_id = policy.cpl_id)\n" +
            "join commodity on (cpl.commodity_id = commodity.commodity_id)"
    )

    ;



    private String query;
    Query(String query) {
        this.query = query;
    }

    @Override
    public String toString() {
        return query;
    }
}
