package org.fao.ess.d3s.cache.postgres.massive;

import org.postgresql.ds.PGPoolingDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class AdamMassiveQueryTestPool implements Source {

    private PGPoolingDataSource pool;
    public AdamMassiveQueryTestPool(String host, String port, String database, String usr, String psw, int maxConnections) {
        pool = new PGPoolingDataSource();
        if (maxConnections>0) {
            pool.setMaxConnections(maxConnections);
            pool.setInitialConnections(Math.min(100,maxConnections));
        } else {
            pool.setInitialConnections(100);
        }

        pool.setDataSourceName("D3S_Dataset_Cache_L1");
        pool.setServerName(host);
        pool.setPortNumber(port!=null && port.trim().length()>0 ? Integer.parseInt(port) : 5432);
        pool.setDatabaseName(database);
        pool.setUser(usr);
        pool.setPassword(psw);
    }
    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = pool.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }
    @Override
    public void close() {
        if (pool!=null)
            pool.close();
    }




}
