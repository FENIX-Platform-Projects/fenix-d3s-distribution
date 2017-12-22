--Set sampstat_id column value

select DISTINCT sampstat from sdg.wiews_2014;
UPDATE sdg.wiews_2016 SET sampstat = '100) Wild' WHERE sampstat = '100) Wild ';
UPDATE sdg.wiews_2014 SET sampstat = '100) Wild' WHERE sampstat = '100) Wild ';


UPDATE sdg.wiews_2016 SET sampstat_id = '1064' WHERE sampstat = '100) Wild';
UPDATE sdg.wiews_2016 SET sampstat_id = '1065' WHERE sampstat = '200) Weedy';
UPDATE sdg.wiews_2016 SET sampstat_id = '1066' WHERE sampstat = '300) Traditional cultivar/Landrace';
UPDATE sdg.wiews_2016 SET sampstat_id = '1067' WHERE sampstat = '400) Breeding/research material';
UPDATE sdg.wiews_2016 SET sampstat_id = '1068' WHERE sampstat = '500) Advanced/Improved cultivar';
UPDATE sdg.wiews_2016 SET sampstat_id = '1069' WHERE sampstat = '600) GMO';
UPDATE sdg.wiews_2016 SET sampstat_id = '0' WHERE sampstat ISNULL OR sampstat='';


UPDATE sdg.wiews_2014 SET sampstat_id = '1064' WHERE sampstat = '100) Wild';
UPDATE sdg.wiews_2014 SET sampstat_id = '1065' WHERE sampstat = '200) Weedy';
UPDATE sdg.wiews_2014 SET sampstat_id = '1066' WHERE sampstat = '300) Traditional cultivar/Landrace';
UPDATE sdg.wiews_2014 SET sampstat_id = '1067' WHERE sampstat = '400) Breeding/research material';
UPDATE sdg.wiews_2014 SET sampstat_id = '1068' WHERE sampstat = '500) Advanced/Improved cultivar';
UPDATE sdg.wiews_2014 SET sampstat_id = '1069' WHERE sampstat = '600) GMO';
UPDATE sdg.wiews_2014 SET sampstat_id = '0' WHERE sampstat ISNULL OR sampstat='';

--Set w_species column value

UPDATE sdg.wiews_2016 SET w_species = genus||' '||species;
UPDATE sdg.wiews_2014 SET w_species = genus||' '||species;

