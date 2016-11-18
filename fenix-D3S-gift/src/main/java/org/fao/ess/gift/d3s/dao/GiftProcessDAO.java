package org.fao.ess.gift.d3s.dao;

import org.fao.ess.gift.d3s.dto.DatasetType;
import org.fao.ess.gift.d3s.dto.Items;
import org.fao.ess.gift.d3s.dto.Queries;
import org.fao.ess.gift.d3s.utils.DataSource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class GiftProcessDAO extends WDSDatasetDao {
    @Inject private DataSource dataSource;
    private boolean initialized = false;

    @Override
    public boolean init() {
        return !initialized;
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
        if (!initialized)
            dataSource.init(properties.get("url"),properties.get("usr"),properties.get("psw"));
        initialized = true;
    }

    @Override
    public Iterator<Object[]> loadData(MeIdentification resource) throws Exception {
        DatasetType datasetType = getType(resource.getUid());
        String survey = getSurvey(datasetType,resource.getUid());
        if (survey==null)
            throw new UnsupportedOperationException("Dataset uid syntaxt not supported: "+resource.getUid());

        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(
                buildQuery(connection, resource, survey, datasetType),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.FETCH_FORWARD);
        fillStatement(resource, survey, datasetType, statement);
        statement.setFetchSize(100);

        return new DataIterator(statement.executeQuery(),connection,null,null);
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException();
    }

    //Query management methods
    private String buildQuery(Connection connection, MeIdentification resource, String survey, DatasetType datasetType) throws Exception {
        switch (datasetType) {
            case dailySubjectAvgBySubgroup:
            case subgroupSubjectTotal:
                return Queries.loadSubgroupDailySubjectAvg.getQuery();

            case foodSubjectTotal:
                return Queries.loadFoodDailySubject.getQuery();
            case foodSubjectTotalWeighted:
                return Queries.loadFoodDailySubjectWeighted.getQuery().replace("<<subjects>>", String.valueOf(countSubject(connection, survey)));
            case foodSubjectRoundTotal:
                return Queries.loadFoodDailySubjectRound.getQuery();
            case foodSubjectRoundTotalWeighted:
                return Queries.loadFoodDailySubjectRoundWeighted.getQuery().replace("<<subjects>>", String.valueOf(countSubject(connection, survey)));
            default:
                return null;
        }
    }
    private int countSubject(Connection connection, String survey) throws Exception {
        PreparedStatement statement = connection.prepareStatement(Queries.countSubject.getQuery());
        statement.setString(1, survey);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        return resultSet.getInt(1);
    }
    private int countSubjectRound(Connection connection, String survey) throws Exception {
        PreparedStatement statement = connection.prepareStatement(Queries.countSubjectRound.getQuery());
        statement.setString(1, survey);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        return resultSet.getInt(1);
    }

    private void fillStatement(MeIdentification resource, String survey, DatasetType datasetType, PreparedStatement statement) throws Exception {
        switch (datasetType) {
            case dailySubjectAvgBySubgroup:
            case subgroupSubjectTotal:
                //for (int i=1; i<=42; i++)
                for (int i=1; i<=4; i++)
                    statement.setString(i, survey);
                break;

            case foodSubjectTotal:
            case foodSubjectTotalWeighted:
                for (int i=1; i<=31; i++)
                    statement.setString(i, survey);
                break;
            case foodSubjectRoundTotal:
            case foodSubjectRoundTotalWeighted:
                for (int i=1; i<=3; i++)
                    statement.setString(i, survey);
                break;
        }
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
/*

    //@Inject private DatabaseUtils databaseUtils;
    //@Inject private Resources resourceService;
    //@Inject private UIDUtils uidUtils;



    private static final String[] tableColumns = new String[]{ "adm0_code", "adm2_code", "gps", "weighting_factor", "household", "subject", "gender", "birth_date", "age_year", "age_month", "first_ant_date", "weight", "height", "method_first_weight", "method_first_height", "second_ant_date", "sweight", "sheight", "method_second_weight", "method_second_height", "special_diet", "special_condition", "energy_intake", "unoverrep", "activity", "education", "ethnic", "profession", "survey_day", "consumption_date", "week_day", "exception_day", "consumption_time", "meal", "place", "eat_seq", "food_type", "recipe_code", "recipe_descr", "amount_recipe", "ingredient", "group_code", "subgroup_code", "foodex2_code", "facet_a", "facet_b", "facet_c", "facet_d", "facet_e", "facet_f", "facet_g", "item", "value", "um" };
    private static final int[] tableColumnsJdbcType = new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DATE, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR};
    private static final Map<String, Integer> tableColumnsIndex = new HashMap<>();
    static { for (int i = 0; i< tableColumns.length; i++) tableColumnsIndex.put(tableColumns[i], i); }




    Collection<DSDColumn> columns = ((DSDDataset)resource.getDsd()).getColumns();
    Items item = getItem(resource.getUid());
    StringBuilder select = new StringBuilder();

        for (DSDColumn column : columns) {
                String columnId = column.getId();
                if (columnId.equalsIgnoreCase("item")) {
                columnId = '\''+item.toString()+'\''+" as item";
                } else if (columnId.equalsIgnoreCase("value")) {
                columnId = item.toString()+" as value";
                } else if (columnId.equalsIgnoreCase("um")) {
                columnId = '\''+item.getUm()+'\''+" as um";
                } else if (columnId.equalsIgnoreCase("ADM0_CODE") || columnId.equalsIgnoreCase("ADM1_CODE") || columnId.equalsIgnoreCase("ADM2_CODE") || columnId.equalsIgnoreCase("SUBJECT") || columnId.equalsIgnoreCase("SURVEY_CODE")) {
                columnId = "S."+columnId;
                } else {
                int type = tableColumnsJdbcType[tableColumnsIndex.get(columnId)];
                if (type == Types.DATE)
                columnId = "to_number(to_char(" + columnId + ", 'YYYYMMDD'), '99999999')";
                else if (type == Types.TIMESTAMP)
                columnId = "to_number(to_char(" + columnId + ", 'YYYYMMDDHH24MISS'), '99999999999999')";
                }
                select.append(',').append(columnId);
                }

                return "select "+select.substring(1)+" from SUBJECT S join CONSUMPTION C on (S.SURVEY_CODE = C.SURVEY_CODE and S.SUBJECT = C.SUBJECT) WHERE S.SURVEY_CODE = ?";
*/