create table  data_fenix as select * from(
	select
	to_char(dt.gaul0code, '9999') as countrycode ,
	g0.name as countrycode_EN,
	to_char(dt.citycode, '9999') as citycode,
	cy.name as citycode_EN ,
	dt.marketcode as  marketcode ,
	mk.name  as marketcode_EN  ,
	dt.vendorcode as vendorcode  ,
	dt.vendorname as vendorcode_EN ,
	dt.munitcode as um  ,
	mu.name as um_EN  ,
	dt.currencycode as currencycode ,
	cu.name ascurrencycode_EN  ,
	dt.commoditycode as commoditycode ,
	cm.name as commoditycode_EN ,
	dt.varietycode as varietycode ,
	va.name as varietycode_EN ,
	dt.price as price  ,
	 dt.date as day,
	dt.note as note  ,
	dt.quantity  as quantity


	from  (select * from data where gaul0code='90')dt,
	 (select * from gaul0 where code='90')g0,
	 city as cy,
	 market as mk,
	 munit as mu,
		currency as cu,
		variety as va,
		commodity as cm

	where
	dt.citycode = cy.code and
	cast(dt.marketcode as integer)= mk.code and
	dt.munitcode = mu.code and
	dt.currencycode = cu.code and
	cast(dt.commoditycode as integer) =cm.code and
	dt.varietycode = va.code
)t