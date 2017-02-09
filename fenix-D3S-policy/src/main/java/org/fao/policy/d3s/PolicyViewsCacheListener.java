package org.fao.policy.d3s;

import org.fao.fenix.commons.utils.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

@Context({"oecd_view"})
public class PolicyViewsCacheListener implements DatasetCacheListener {

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        //Browse Data indexes
        //datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( oda,sectorcode, purposecode)");

        String uid = datasetInfo.getMetadata().getUid();

        switch (uid) {

            case "OECD_View_ImportTariffs":

                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( hsPrefix,policytype)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policytype)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype,country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startdate)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( enddate)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startYear)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( endYear)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( um)");
                break;

            case "OECD_View_BiofuelPolicy":

                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policytype)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policymeasure)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policymeasure,country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype,country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startdate)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( enddate)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( startYear)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( endYear)");
                break;


            case "OECD_Timeseries_BiofuelPolicy":
            case "OECD_Timeseries_ExportRestrictions" :

                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( day,policytype, country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( day,policymeasure, country)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass,policytype)");

                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype,policymeasure)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policymeasure)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( country)");
                break;

            case "OECD_Policy_Master" :
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( cpl_id, policy_id )");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commoditydomain,policydomain,country,commodityclass,policytype,policymeasure)");
                break;

            case "OECD_View_QueryDownload" :
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commoditydomain, commodityclass )");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( policytype, policymeasure )");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass_order )");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on " + datasetInfo.getTableName() + " ( commodityclass_mixed )");

                break;
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
