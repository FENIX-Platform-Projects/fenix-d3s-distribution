package org.fao.policy.d3s;

import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

@Context({"oecd_view"})
public class PolicyViewsCacheListener implements DatasetCacheListener {

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        //Browse Data indexes
        //datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( oda,sectorcode, purposecode)");

        String uid = datasetInfo.getMetadata().getUid();


        if ("OECD_View_ImportTariffs".equals(uid)) {
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( hsPrefix,policytype)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policytype)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype,country)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startdate)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( enddate)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startYear)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( endYear)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( um)");


        } else if ("OECD_View_BiofuelPolicy".equals(uid)) {

            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policytype)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policymeasure)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policymeasure,country)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype,country)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startdate)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( enddate)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startYear)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( endYear)");

        } else if("OECD_Timeseries_BiofuelPolicy".equals(uid) || "OECD_Timeseries_ExportRestrictions".equals(uid)) {


            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( day,policytype, country)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( day,policymeasure, country)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policytype)");

            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype,policymeasure)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policymeasure)");
            datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( country)");
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
