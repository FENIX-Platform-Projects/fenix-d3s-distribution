package org.fao.ess.wiews.d3s.dao;

import org.fao.ess.wiews.d3s.dto.DatasetType;
import org.fao.ess.wiews.d3s.dto.Queries;
import org.fao.ess.wiews.d3s.utils.DataSource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class WiewsProcessDAO extends WDSDatasetDao {
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
            //current
            case foodSubjectDailyTotal:
                return Queries.loadFoodDailyTotalSubject.getQuery();
            case foodSubjectTotal:
                return Queries.loadFoodDailySubject.getQuery();
            case foodSubjectTotalWeighted:
                return Queries.loadFoodDailySubjectWeighted.getQuery().replace("<<subjects>>", String.valueOf(countSubject(connection, survey)));

            //future
            case foodSubjectConsumption:
                return Queries.loadFoodSubject.getQuery();
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
            //current
            case foodSubjectTotal:
                for (int i=1; i<=32; i++)
                    statement.setString(i, survey);
                break;
            case foodSubjectTotalWeighted:
                for (int i=1; i<=15; i++)
                    statement.setString(i, survey);
                break;
            case foodSubjectDailyTotal:
                for (int i=1; i<=2; i++)
                    statement.setString(i, survey);
                break;

            //future
            case foodSubjectConsumption:
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
    private String getSurvey(DatasetType datasetType, String uid) {
        return uid.substring(datasetType.getPrefix().length());
    }




}
