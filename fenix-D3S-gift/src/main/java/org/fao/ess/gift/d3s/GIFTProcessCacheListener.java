package org.fao.ess.gift.d3s;

import org.fao.ess.gift.d3s.dto.DatasetType;
import org.fao.ess.gift.d3s.dto.Items;
import org.fao.ess.gift.d3s.dto.Queries;
import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

import java.sql.Connection;
import java.sql.ResultSet;

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
            case subgroupSubjectTotal:
                datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (group_code, subgroup_code)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (item, group_code, subgroup_code)");
                return false;

            case foodSubjectTotal:
            case foodSubjectRoundTotal:
            case foodSubjectTotalWeighted:
            case foodSubjectRoundTotalWeighted:
                datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (group_code, subgroup_code, foodex2_code)");
                datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (item, group_code, subgroup_code)");
                return false;
            default:
                return false;
        }
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) throws Exception {
        String uid = datasetInfo.getMetadata().getUid();
        DatasetType datasetType = getType(uid);
        String survey = getSurvey(datasetType,uid);
        if (survey==null)
            throw new UnsupportedOperationException("Dataset uid syntaxt not supported: "+uid);

        Connection connection = datasetInfo.getConnection();
        String tableName = datasetInfo.getTableName();
        switch (datasetType) {
            case dailySubjectAvgBySubgroup:

            case foodSubjectTotalWeighted:
                connection.createStatement().executeUpdate("UPDATE "+tableName+" SET value = value/"+ countSubject(connection, tableName));
                break;
            case foodSubjectRoundTotalWeighted:
                connection.createStatement().executeUpdate("UPDATE "+tableName+" SET value = value/"+ countSubjectRound(connection, tableName));
                break;
        }
        return false;
    }

    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }

    private int countSubject(Connection connection, String tableName) throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery(Queries.countSubject.getQuery().replace("<<tableName>>", tableName));
        resultSet.next();
        return resultSet.getInt(1);
    }
    private int countSubjectRound(Connection connection, String tableName) throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery(Queries.countSubjectRound.getQuery().replace("<<tableName>>", tableName));
        resultSet.next();
        return resultSet.getInt(1);
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
