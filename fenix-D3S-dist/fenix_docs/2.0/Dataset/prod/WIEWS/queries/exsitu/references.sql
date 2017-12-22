--ref_sdg_species
INSERT INTO sdg.ref_sdg_species(year, genus, species)
  SELECT 2016 AS year, genus, species FROM sdg.wiews_2016 GROUP BY genus, species
  UNION ALL
  SELECT 2014 AS year, genus, species FROM sdg.wiews_2014 GROUP BY genus, species;

--ref_sdg_taxon
INSERT INTO sdg.ref_sdg_taxon (year, taxon)
  SELECT 2016 AS year, taxon FROM sdg.wiews_2016 GROUP BY taxon
  UNION ALL
  SELECT 2014 AS year, taxon FROM sdg.wiews_2014 GROUP BY taxon;

--ref_sdg_institutes
INSERT INTO sdg.ref_sdg_institutes (year, w_institute, w_institute_en)
  SELECT year, w_instcode, CASE WHEN orgname_l ISNULL THEN w_instcode ELSE w_instcode||' - '||orgname_l END
  FROM (SELECT 2016 AS year, w_instcode FROM sdg.wiews_2016 GROUP BY w_instcode) data_2016
  LEFT JOIN ref_instab ON (w_instcode = wiews_instcode)
  UNION ALL
  SELECT year, w_instcode, CASE WHEN orgname_l ISNULL THEN w_instcode ELSE w_instcode||' - '||orgname_l END
  FROM (SELECT 2014 AS year, w_instcode FROM sdg.wiews_2014 GROUP BY w_instcode) data_2014
  LEFT JOIN ref_instab ON (w_instcode = wiews_instcode);

SELECT count(*) FROM sdg.ref_sdg_species; --102092
SELECT count(*) FROM sdg.ref_sdg_taxon; --129134
SELECT count(*) FROM sdg.ref_sdg_institutes; --1166
