package org.fao.ess.wiews.d3s.dao;

import org.fao.fenix.commons.utils.Context;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

import javax.inject.Inject;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;

@Context({"wiews"})
public class IndicatorCacheListener implements DatasetCacheListener {
    @Inject UIDUtils uidUtils;

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {

        String uid = datasetInfo.getMetadata().getUid();

        switch (uid) {

            case "raw_indicator3" :
            case "raw_indicator20" :
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( iteration, country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( country, iteration)");
                break;
            case "indicator3" :
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( INDICATOR , iteration, wiews_region)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( INDICATOR , iteration, country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( INDICATOR ,iteration)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( INDICATOR ,wiews_region)");
                break;
            case "indicator20":
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( iteration, element, wiews_region )");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( iteration, element, genus )");
                break;
        }
        return false;
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) throws Exception {
        String uid = datasetInfo.getMetadata().getUid();
        //Indicator 20
/*        try {
            if (uid.equals("indicator20")) {
                String tableName = datasetInfo.getTableName();
                String tmpTableName = "tmp_"+uidUtils.getId();
                String regionMappingTableName = "data.wiews_regions_mapping";
                //Add m49_country values
                datasetInfo.getConnection().createStatement().executeUpdate("create table "+tmpTableName+" as select * from " + tableName);
                datasetInfo.getConnection().createStatement().executeUpdate("truncate table " + tableName);
                datasetInfo.getConnection().createStatement().executeUpdate(
                        "insert into " + tableName + " (iteration, domain, element, indicator, biologicalaccessionid, country, m49_country, stakeholder, genus, value, um, iteration_en, iteration_es, iteration_fr, iteration_de, domain_en, domain_es, domain_fr, domain_de, element_en, element_es, element_fr, element_de, genus_en, genus_es, genus_fr, genus_de, indicator_en, indicator_es, indicator_fr, indicator_de, biologicalaccessionid_en, biologicalaccessionid_es, biologicalaccessionid_fr, biologicalaccessionid_de, country_en, country_es, country_fr, country_de, m49_country_en, m49_country_es, m49_country_fr, m49_country_de, stakeholder_en, stakeholder_es, stakeholder_fr, stakeholder_de, um_en, um_es, um_fr, um_de)\n" +
                                "select iteration, domain, element, indicator, biologicalaccessionid, i.country, m.m49_country, stakeholder, genus, value, um, iteration_en, iteration_es, iteration_fr, iteration_de, domain_en, domain_es, domain_fr, domain_de, element_en, element_es, element_fr, element_de, genus_en, genus_es, genus_fr, genus_de, indicator_en, indicator_es, indicator_fr, indicator_de, biologicalaccessionid_en, biologicalaccessionid_es, biologicalaccessionid_fr, biologicalaccessionid_de, i.country_en, i.country_es, i.country_fr, i.country_de, m.m49_country_en, m.m49_country_es, m.m49_country_fr, m.m49_country_de, stakeholder_en, stakeholder_es, stakeholder_fr, stakeholder_de, um_en, um_es, um_fr, um_de\n" +
                                "from "+tmpTableName+" i join " + regionMappingTableName + " m on (m.country = i.country)"
                );
                //Add world indicator
                datasetInfo.getConnection().createStatement().executeUpdate(
                        "insert into " + tableName + " (iteration, domain, element, indicator, biologicalaccessionid, country, m49_country, stakeholder, genus, value, um, iteration_en, iteration_es, iteration_fr, iteration_de, domain_en, domain_es, domain_fr, domain_de, element_en, element_es, element_fr, element_de, genus_en, genus_es, genus_fr, genus_de, indicator_en, indicator_es, indicator_fr, indicator_de, biologicalaccessionid_en, biologicalaccessionid_es, biologicalaccessionid_fr, biologicalaccessionid_de, country_en, country_es, country_fr, country_de, m49_country_en, m49_country_es, m49_country_fr, m49_country_de, stakeholder_en, stakeholder_es, stakeholder_fr, stakeholder_de, um_en, um_es, um_fr, um_de)\n" +
                                "select max(iteration), max(domain), 'ind' as element, max(indicator), null as biologicalaccessionid, null as country, '1' as m49_country, 'ZZZ' as stakeholder, 'na' as genus, sum(value), max(um), max(iteration_en), max(iteration_es), max(iteration_fr), max(iteration_de), max(domain_en), max(domain_es), max(domain_fr), max(domain_de), 'Indicator' as element_en, null as element_es, null as element_fr, null as element_de, null as genus_en, null as genus_es, null as genus_fr, null as genus_de, max(indicator_en), max(indicator_es), max(indicator_fr), max(indicator_de), null as biologicalaccessionid_en, null as biologicalaccessionid_es, null as biologicalaccessionid_fr, null as biologicalaccessionid_de, null as country_en, null as country_es, null as country_fr, null as country_de, 'World' as m49_country_en, null as m49_country_es, null as m49_country_fr, null as m49_country_de, null as stakeholder_en, null as stakeholder_es, null as stakeholder_fr, null as stakeholder_de, max(um_en), max(um_es), max(um_fr), max(um_de)\n" +
                                "from "+tmpTableName+" where element = 'stk' and stakeholder not in ('BEL084','CIV033','COL003','ETH013','IND002','KEN056','MEX002','NGA039','PER001','PHL001','SYR002','TWN001','FJI049','SWE054')"
                );
                //Add world focal point rating average
                datasetInfo.getConnection().createStatement().executeUpdate(
                        "insert into " + tableName + " (iteration, domain, element, indicator, biologicalaccessionid, country, m49_country, stakeholder, genus, value, um, iteration_en, iteration_es, iteration_fr, iteration_de, domain_en, domain_es, domain_fr, domain_de, element_en, element_es, element_fr, element_de, genus_en, genus_es, genus_fr, genus_de, indicator_en, indicator_es, indicator_fr, indicator_de, biologicalaccessionid_en, biologicalaccessionid_es, biologicalaccessionid_fr, biologicalaccessionid_de, country_en, country_es, country_fr, country_de, m49_country_en, m49_country_es, m49_country_fr, m49_country_de, stakeholder_en, stakeholder_es, stakeholder_fr, stakeholder_de, um_en, um_es, um_fr, um_de)\n" +
                                "select max(iteration), max(domain), 'nfpa' as element, max(indicator), null as biologicalaccessionid, null as country, '1' as m49_country, 'ZZZ' as stakeholder, 'na' as genus, avg(value), max(um), max(iteration_en), max(iteration_es), max(iteration_fr), max(iteration_de), max(domain_en), max(domain_es), max(domain_fr), max(domain_de), 'National Focal Point rating, average' as element_en, null as element_es, null as element_fr, null as element_de, null as genus_en, null as genus_es, null as genus_fr, null as genus_de, max(indicator_en), max(indicator_es), max(indicator_fr), max(indicator_de), null as biologicalaccessionid_en, null as biologicalaccessionid_es, null as biologicalaccessionid_fr, null as biologicalaccessionid_de, null as country_en, null as country_es, null as country_fr, null as country_de, 'World' as m49_country_en, null as m49_country_es, null as m49_country_fr, null as m49_country_de, null as stakeholder_en, null as stakeholder_es, null as stakeholder_fr, null as stakeholder_de, max(um_en), max(um_es), max(um_fr), max(um_de)\n" +
                                "from "+tmpTableName+" where element = 'nfp'"
                );
                //Add world + international stakeholders indicator
                datasetInfo.getConnection().createStatement().executeUpdate(
                        "insert into " + tableName + " (iteration, domain, element, indicator, biologicalaccessionid, country, m49_country, stakeholder, genus, value, um, iteration_en, iteration_es, iteration_fr, iteration_de, domain_en, domain_es, domain_fr, domain_de, element_en, element_es, element_fr, element_de, genus_en, genus_es, genus_fr, genus_de, indicator_en, indicator_es, indicator_fr, indicator_de, biologicalaccessionid_en, biologicalaccessionid_es, biologicalaccessionid_fr, biologicalaccessionid_de, country_en, country_es, country_fr, country_de, m49_country_en, m49_country_es, m49_country_fr, m49_country_de, stakeholder_en, stakeholder_es, stakeholder_fr, stakeholder_de, um_en, um_es, um_fr, um_de)\n" +
                                "select max(iteration), max(domain), 'ind' as element, max(indicator), null as biologicalaccessionid, null as country, 'WITC' as m49_country, 'ZZZ' as stakeholder, 'na' as genus, sum(value), max(um), max(iteration_en), max(iteration_es), max(iteration_fr), max(iteration_de), max(domain_en), max(domain_es), max(domain_fr), max(domain_de), 'Indicator' as element_en, null as element_es, null as element_fr, null as element_de, null as genus_en, null as genus_es, null as genus_fr, null as genus_de, max(indicator_en), max(indicator_es), max(indicator_fr), max(indicator_de), null as biologicalaccessionid_en, null as biologicalaccessionid_es, null as biologicalaccessionid_fr, null as biologicalaccessionid_de, null as country_en, null as country_es, null as country_fr, null as country_de, 'World + International Centers' as m49_country_en, null as m49_country_es, null as m49_country_fr, null as m49_country_de, null as stakeholder_en, null as stakeholder_es, null as stakeholder_fr, null as stakeholder_de, max(um_en), max(um_es), max(um_fr), max(um_de)\n" +
                                "from "+tmpTableName+" where element = 'stk'"
                );
                //Drop temporary table
                datasetInfo.getConnection().createStatement().executeUpdate("drop table "+tmpTableName);
                //Create indexes
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + tableName + " ( element,country )");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
*/        return false;
    }

    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }

}
