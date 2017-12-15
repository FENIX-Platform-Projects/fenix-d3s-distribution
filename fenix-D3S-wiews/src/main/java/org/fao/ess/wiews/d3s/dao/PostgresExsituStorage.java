package org.fao.ess.wiews.d3s.dao;

import org.fao.ess.d3s.cache.postgres.PostgresDefaultStorage;
import org.fao.fenix.d3s.cache.storage.StorageName;
import org.fao.fenix.d3s.cache.tools.Server;

import javax.inject.Singleton;

@Singleton
@StorageName("exsitu")
public class PostgresExsituStorage extends PostgresDefaultStorage {
    private static String SCHEMA_NAME = "sdg";

    //STARTUP FLOW
    @Override
    public void open() throws Exception {
        open(
                "/org/fao/ess/d3s/cache/postgres/config/postgres_wiews_exsitu.properties",
                "file:"+ Server.CONFIG_FOLDER_PATH + "postgres_wiews_exsitu.properties"
        );
    }

    @Override
    public String getTableName(String tableName) {
        return tableName==null ? null : SCHEMA_NAME + '.'+ '"' + tableName + '"';
    }
}

