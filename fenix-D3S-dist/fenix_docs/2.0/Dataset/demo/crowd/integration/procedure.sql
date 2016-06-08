create table  data_fenix as select * from (

  select
    to_char(dt.gaul0code, '9999') as countrycode ,
    to_char(dt.citycode, '9999') as citycode,
    dt.marketcode as  marketcode ,
    dt.vendorcode as vendorcode  ,
    dt.munitcode as unitcode  ,
    dt.currencycode as currencycode ,
    dt.commoditycode as commoditycode ,
    avg(dt.price) as price  ,
    cast(to_char(dt.date, 'YYYYMMDD') as integer) as date,
    dt.quantity  as quantity,

    g0.name as countrycode_en,
    cast(null as  character varying(80)) as countrycode_es,
    cast(null as character varying(80)) as countrycode_fr,
    cast(null as  character varying(80)) as countrycode_de,

    cy.name as citycode_en ,
    cast(null as   character varying(80)) as citycode_es  ,
    cast(null as   character varying(80)) as citycode_fr ,
    cast(null as   character varying(80)) as citycode_de ,

    mk.name  as marketcode_en  ,
    cast(null as   character varying(80)) as marketcode_fr,
    cast(null as   character varying(80)) as marketcode_es,
    cast(null as   character varying(80)) as marketcode_de,

    ve.name as vendorcode_en ,
    cast(null as character varying(80)) as vendorcode_fr ,
    cast(null as   character varying(80)) as vendorcode_es ,
    cast(null as   character varying(80)) as vendorcode_de ,

    mu.name as unitcode_en  ,
    cast(null as   character varying(80)) as unitcode_fr  ,
    cast(null as   character varying(80)) as unitcode_es  ,
    cast(null as   character varying(80)) as unitcode_de  ,

    cu.name as currencycode_en  ,
    cast(null as   character varying(80)) as currencycode_fr  ,
    cast(null as   character varying(80)) as currencycode_es  ,
    cast(null as   character varying(80)) as currencycode_de  ,

    cm.name as commoditycode_en ,
    cast(null as character varying(80)) as commoditycode_fr ,
    cast(null as   character varying(80)) as commoditycode_es ,
    cast(null as   character varying(80)) as commoditycode_de

    from
      (select gaul0code,citycode,marketcode, vendorcode,munitcode,
       commoditycode,currencycode,date, quantity, avg(price) as price
       from data where gaul0code='90'
       group by data.gaul0code,data.citycode,data.marketcode, data.vendorcode,data.munitcode,
       data.commoditycode,data.currencycode,data.date, data.quantity )dt,
      (select * from gaul0 where code='90')g0,
      city as cy,
      market as mk,
      munit as mu,
      currency as cu,
      commodity as cm,
      vendor as ve

    where
      dt.citycode = cy.code and
      cast(dt.marketcode as integer)= mk.code and
      dt.munitcode = mu.code and
      dt.currencycode = cu.code and
      cast(dt.commoditycode as integer) =cm.code and
      cast(dt.vendorcode as integer) =ve.code

    group by dt.gaul0code,dt.citycode,dt.marketcode, dt.vendorcode,dt.munitcode,
     dt.commoditycode,dt.currencycode,dt.date, dt.quantity,
     g0.name, cy.name,mk.name,dt.vendorcode, ve.name, mu.name,
     cu.name, cm.name


)t