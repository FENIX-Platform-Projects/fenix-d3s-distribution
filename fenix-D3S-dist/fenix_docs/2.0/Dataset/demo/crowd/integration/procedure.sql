create table  data_fenix as select * from (

	select
	to_char(dt.gaul0code, '9999') as countrycode ,
	g0.name as countrycode_EN,
	cast(null as  character varying(80)) as countrycode_ES,
	cast(null as character varying(80)) as countrycode_FR,
	cast(null as  character varying(80)) as countrycode_DE,

	to_char(dt.citycode, '9999') as citycode,
	cy.name as citycode_EN ,
	cast(null as   character varying(80)) as citycode_ES  ,
	cast(null as   character varying(80)) as citycode_FR ,
	cast(null as   character varying(80)) as citycode_DE ,

	dt.marketcode as  marketcode ,
	mk.name  as marketcode_EN  ,
	cast(null as   character varying(80)) as marketcode_FR,
	cast(null as   character varying(80)) as marketcode_ES,
	cast(null as   character varying(80)) as marketcode_DE,

	dt.vendorcode as vendorcode  ,
	dt.vendorname as vendorcode_EN ,
	cast(null as character varying(80)) as vendorcode_FR ,
	cast(null as   character varying(80)) as vendorcode_ES ,
	cast(null as   character varying(80)) as vendorcode_DE ,

	dt.munitcode as um  ,
	mu.name as um_EN  ,
	cast(null as   character varying(80)) as um_FR  ,
	cast(null as   character varying(80)) as um_ES  ,
	cast(null as   character varying(80)) as um_DE  ,
	dt.currencycode as currencycode ,

	cu.name as currencycode_EN  ,
	cast(null as   character varying(80)) as currencycode_FR  ,
	cast(null as   character varying(80)) as currencycode_ES  ,
	cast(null as   character varying(80)) as currencycode_DE  ,

	dt.commoditycode as commoditycode ,
	cm.name as commoditycode_EN ,
	cast(null as character varying(80)) as commoditycode_FR ,
	cast(null as   character varying(80)) as commoditycode_ES ,
	cast(null as   character varying(80)) as commoditycode_DE ,

	dt.price as price  ,
	dt.date as day,
	dt.note as note  ,
	dt.quantity  as quantity

	from
	(select * from data where gaul0code='90')dt,
	(select * from gaul0 where code='90')g0,
	city as cy,
	market as mk,
	munit as mu,
	currency as cu,
	commodity as cm

	where
	dt.citycode = cy.code and
	cast(dt.marketcode as integer)= mk.code and
	dt.munitcode = mu.code and
	dt.currencycode = cu.code and
	cast(dt.commoditycode as integer) =cm.code



)t