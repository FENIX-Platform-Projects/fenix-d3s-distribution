package org.fao.ess.d3s.cache.postgres;

import org.apache.log4j.Logger;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.cache.tools.Server;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.RunScript;
import org.postgresql.ds.PGPoolingDataSource;

import javax.inject.Inject;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public abstract class PostgresStorage implements DatasetStorage {
    private static final Logger LOGGER = Logger.getLogger(PostgresStorage.class);


    //CONNECTION MANAGEMENT
    /*
    private PGPoolingDataSource pool;
    private void initPool(String host, String port, String database, String usr, String psw, int maxConnections) {
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

*/
    private String url, usr, psw;
    public void initPool(String host, String port, String database, String usr, String psw, int maxConnections) {
        this.url = "jdbc:postgresql://"+host+':'+(port!=null && port.trim().length()>0 ? port : "5432")+'/'+database;
        this.usr = usr;
        this.psw = psw;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(url,usr,psw);
        connection.setAutoCommit(false);
        return connection;
    }
    @Override
    public void close() {
    }

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
