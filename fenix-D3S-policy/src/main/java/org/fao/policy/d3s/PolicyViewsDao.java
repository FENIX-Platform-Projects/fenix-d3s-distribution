package org.fao.policy.d3s;

import org.fao.fenix.commons.msd.dto.full.DSD;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.*;
import java.util.*;

public class PolicyViewsDao extends WDSDatasetDao {

    @Inject private DataSource dataSource;
    @Inject private DatabaseUtils databaseUtils;
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

        Query query = Query.valueOf(resource.getUid());
        if (query==null)
            throw new UnsupportedOperationException("No support for the requested dataset: "+resource.getUid());

        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(1000);
        return new DataIterator(
                statement.executeQuery(query.toString()),
                connection,
                null,
                null
        );
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }


    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException("Operation supported only for topic datasets");
    }






}
