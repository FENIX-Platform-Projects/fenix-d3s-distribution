package org.fao.ess.d3s.cache.postgres.massive;

import java.sql.Connection;
import java.sql.SQLException;

public interface Source {

    Connection getConnection() throws SQLException;
    void close();

}
