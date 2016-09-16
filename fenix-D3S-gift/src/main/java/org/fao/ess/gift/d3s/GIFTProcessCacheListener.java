package org.fao.ess.gift.d3s;

import org.fao.ess.gift.d3s.dto.DatasetType;
import org.fao.ess.gift.d3s.dto.Items;
import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

@Context({"gift_process"})
public class GIFTProcessCacheListener implements DatasetCacheListener {

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        String uid = datasetInfo.getMetadata().getUid();
        DatasetType datasetType = getType(uid);
        String survey = getSurvey(datasetType,uid);
        if (survey==null)
            throw new UnsupportedOperationException("Dataset uid syntaxt not supported: "+uid);

        switch (datasetType) {
            case dailySubjectAvgBySubgroup:
                datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (group_code, subgroup_code)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (item, group_code, subgroup_code)");
                return false;
            default:
                return false;
        }
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }

    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }


    //Utils
    public DatasetType getType (String uid) {
        for (DatasetType datasetType : DatasetType.values())
            if (uid.startsWith(datasetType.getPrefix()))
                return datasetType;
        return null;
    }
    private Items getItem(String uid) {
        try {
            return Items.valueOf(uid.substring(uid.lastIndexOf('_')+1));
        } catch (Exception ex) {
            return null;
        }
    }
    private String getSurvey(DatasetType datasetType, String uid) {
        int end = uid.indexOf('_', datasetType.getPrefix().length());
        return uid.substring(datasetType.getPrefix().length(), end>0 ? end : uid.length());
    }

}
