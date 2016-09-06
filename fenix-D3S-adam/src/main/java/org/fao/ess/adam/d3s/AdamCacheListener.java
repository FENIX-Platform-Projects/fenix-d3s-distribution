package org.fao.ess.adam.d3s;

import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

@Context({"adam"})
public class AdamCacheListener implements DatasetCacheListener {

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        String uid = datasetInfo.getMetadata().getUid();

        if ("adam_usd_commitment".equals(uid) ||
                   "adam_usd_commitment_defl".equals(uid)
                || "adam_usd_disbursement".equals(uid)
                || "adam_usd_disbursement_defl".equals(uid)) {
            //Generic indexes
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( parentsector_code, year)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( parentsector_code, donorcode)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( parentsector_code, recipientcode)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( purposecode, year)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( purposecode, donorcode)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( purposecode, recipientcode)");
            //Compare-Analyze page
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( year, purposecode)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( year, parentsector_code)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( year, donorcode)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( year, recipientcode)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( year, channelsubcategory_code)");
            // new
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( channelsubcategory_code)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( un_continent_code)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( gaul0)");

        } else if ("adam_country_indicators".equals(uid)) {
            //Generic indexes
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " (countrycode)");
        }
        return false;
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }

    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }
}
