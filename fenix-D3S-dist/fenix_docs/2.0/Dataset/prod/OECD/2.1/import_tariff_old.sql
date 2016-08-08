   SELECT * FROM
   (SELECT policytable.Metadata_ID,
   policytable.Policy_ID,
   policytable.CPL_ID, commlistwithid.Commodity_ID,
   SUBSTRING(commlistwithid.HS_Code from 0 for 5) AS HS_Tariff,
   mastertable.Country_Code,
   mastertable.Country_Name,
   mastertable.Subnational_Code,
   mastertable.Subnational_Name,
   mastertable.CommodityDomain_Code,
   mastertable.CommodityDomain_Name,
   mastertable.CommodityClass_Code,
   mastertable.CommodityClass_Name,
   mastertable.PolicyDomain_Code,
   mastertable.PolicyDomain_Name,
   mastertable.PolicyType_Code,
   mastertable.PolicyType_Name,
   mastertable.PolicyMeasure_Code,
   mastertable.PolicyMeasure_Name,
   mastertable.Condition_Code,
   mastertable.Condition,
   mastertable.IndividualPolicy_Code,
   mastertable.IndividualPolicy_Name,
   policytable.Policy_Element,
   EXTRACT(year FROM policytable.Start_Date) as start_date_year,
   EXTRACT(year FROM policytable.End_Date) as end_date_year,
   policytable.Units,
     policytable.Value,
     policytable.Value_Text,
     policytable.Value_Type,
     policytable.Exemptions,
     policytable.MinAVTariffValue,
     policytable.Notes,
     policytable.Link,
     policytable.Source,
     policytable.Title_Of_Notice,
     policytable.Legal_Basis_Name,
     policytable.Date_Of_Publication,
      policytable.Imposed_End_Date,
      policytable.Second_Generation_Specific,
      policytable.Benchmark_Tax,
       policytable.Benchmark_Product,
       policytable.Tax_Rate_Biofuel,
       policytable.Tax_Rate_Benchmark,
        policytable.Start_Date_Tax,
        policytable.Benchmark_Link,
        policytable.Original_Dataset,
        policytable.Type_Of_Change_Code,
        policytable.Type_Of_Change_Name,
        policytable.Measure_Description,
         policytable.Product_Original_HS,
         policytable.Product_Original_Name,
         policytable.Link_pdf,
         policytable.Benchmark_Link_pdf,
         policytable.Element_Code,
         policytable.MaxAVTariffValue,
         policytable.CountAVTariff,
         policytable.CountNAVTariff
         FROM
          (mastertable INNER JOIN policytable ON mastertable.CPL_ID = policytable.CPL_ID)
          INNER JOIN
          commlistwithid ON mastertable.Commodity_ID = commlistwithid.Commodity_ID
          WHERE
          (SUBSTRING
            (commlistwithid.HS_Code from 0 for 5) IN ('1001','1005','1006','1201') AND
            (policytable.Policy_Element='Final bound tariff' Or policytable.Policy_Element='MFN applied tariff')
             AND (policytable.Units='%')))tot1

right JOIN
          ( SELECT generate_series FROM generate_series
        ((SELECT MIN(CAST(EXTRACT(year FROM policytable.Start_Date) AS INTEGER))
         FROM (mastertable INNER JOIN policytable ON mastertable.CPL_ID = policytable.CPL_ID)
          INNER JOIN commlistwithid ON mastertable.Commodity_ID = commlistwithid.Commodity_ID
          WHERE (SUBSTRING(commlistwithid.HS_Code from 0 for 5) IN ('1001','1005','1006','1201')
          AND (policytable.Policy_Element='Final bound tariff'
          Or policytable.Policy_Element='MFN applied tariff') AND (policytable.Units='%'))),
           2016))tot2

          WHERE tot1.start_date_year<=tot2.generate_series
          AND (tot1.end_date_year Is Null Or tot1.end_date_year>=tot2.generate_series)
