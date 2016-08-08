
select *from (
  select HS_Tariff,commodityclass_name, policy_element, year, units, value from (
	select  * from (
	       select mastertable.commodityclass_name, policytable.policy_element,policy.enddate ,
	       SUBSTRING(commlistwithid.HS_Code from 0 for 5) as hs_tariff,
	       (EXTRACT(year FROM policytable.Start_Date)) as year,
	       commlistwithid.HS_Code, policytable.units, policytable.value
	       from
	       mastertable INNER JOIN policytable ON mastertable.CPL_ID = policytable.CPL_ID
	       INNER JOIN commlistwithid ON mastertable.Commodity_ID = commlistwithid.Commodity_ID)t

    WHERE SUBSTRING(t.HS_Code from 0 for 5) IN ('1001','1005','1006','1201')
        and t.policy_element in ('Final bound tariff','MFN applied tariff')
        and t.units = '%'
        and (t.year <= 2015 and
        (CAST(LEFT(t.enddate, 4) AS INT) is null OR CAST(LEFT(t.enddate, 4) AS INT) >=2015)
        )g)h
