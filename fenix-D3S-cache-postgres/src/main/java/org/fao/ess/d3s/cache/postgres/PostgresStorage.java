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

    @Inject private FileUtils fileUtils;

    private boolean initialized = false;



    //STARTUP FLOW
    @Override
    public void open() throws Exception {
        if (!initialized) {
            Map<String, String> initProperties = org.fao.fenix.commons.utils.Properties.getInstance(
                    "/org/fao/ess/d3s/cache/postgres/config/postgres.properties",
                    "file:"+Server.CONFIG_FOLDER_PATH + "postgres.properties"
            ).toMap();

            initPool(
                    initProperties.get("host"),
                    initProperties.get("port"),
                    initProperties.get("database"),
                    initProperties.get("usr"),
                    initProperties.get("psw"),
                    Integer.parseInt(initProperties.containsKey("max") ? initProperties.get("max") : "0")
            );

            runScript(initProperties.get("ddl"));
            runScript(initProperties.get("dml"));

            initialized = true;
        }
    }

    private void runScript(String resourceFilePath) throws IOException, SQLException {
        if (resourceFilePath!=null && resourceFilePath.trim().length()>0)
            runScript(PostgresStorage.class.getResourceAsStream(resourceFilePath));
    }
    private void runScript(InputStream input) throws IOException, SQLException {
        if (input!=null) {
            //Read script instructions
            String script = fileUtils.readTextFile(input);
            String[] instructions = script.split("-- command");
            //Run safe script
            Connection connection = null;
            for (String command : instructions)
                if ((command = command.trim()).length()>0)
                    try {
                        (connection = getConnection()).setAutoCommit(true);
                        connection.createStatement().executeUpdate(command.trim());
                    } catch (Exception ex) {
                        LOGGER.warn("Postgres storage init script command error: "+ex.getMessage());
                    } finally {
                        if (connection!=null)
                            connection.close();
                    }
        }
    }




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
    private void initPool(String host, String port, String database, String usr, String psw, int maxConnections) {
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
