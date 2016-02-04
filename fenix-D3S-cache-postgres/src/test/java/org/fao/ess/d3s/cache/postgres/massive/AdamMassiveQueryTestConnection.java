package org.fao.ess.d3s.cache.postgres.massive;

import org.postgresql.ds.PGPoolingDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AdamMassiveQueryTestConnection implements Source {

    private String url, usr, psw;
    public AdamMassiveQueryTestConnection(String host, String port, String database, String usr, String psw, int maxConnections) {
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
