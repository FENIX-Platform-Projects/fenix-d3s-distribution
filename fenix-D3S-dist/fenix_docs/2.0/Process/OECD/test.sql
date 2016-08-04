
 SELECT commodityclass,policytype,country,
FIRST('22909405234122408998563555476692079847@@@commodityclass_EN',commodityclass_EN)
AS commodityclass_EN,
FIRST('22909405234122408998563555476692079847@@@policytype_EN',policytype_EN)
AS policytype_EN,FIRST('22909405234122408998563555476692079847@@@country_EN',country_EN)
AS country_EN FROM
(SELECT commodityclass,policytype,country,commodityclass_EN,policytype_EN,country_EN
FROM (SELECT commodityclass,policytype,country,startDate,endDate,policytype_EN,commodityclass_EN,country_EN
FROM (SELECT commodityclass,policytype,country,startDate,endDate,policytype_EN,commodityclass_EN,country_EN
FROM DATA."OECD_View_BiofuelPolicy"
WHERE commodityclass IN ('5','6','7')) as D3P_R_0___54388418004516016477475289465938706768
 WHERE (((startdate BETWEEN 20110101 AND 20140131) OR (enddate BETWEEN 20110101 AND 20140131))OR ((startdate <= 20110101) AND (enddate>=20140131))
 OR (enddate IS NULL AND  startdate>=20110101)))
 as D3P_R_1___41710765478689301378802617284609646326) as D3P_R_2___5703758996427441278364196724043298103
  GROUP BY commodityclass,country,policytype




create table TEST as  (

 SELECT commodityclass,policytype,country,
FIRST('22909405234122408998563555476692079847@@@commodityclass_EN',commodityclass_EN)
AS commodityclass_EN
FROM DATA."OECD_View_BiofuelPolicy"

  GROUP BY commodityclass,country,policytype )