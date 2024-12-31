package com.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * xqy
 * 2024-12-30
 */

public class xx {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createHiveCatalogDDL = "create catalog hive_catalog with(\n"+
                "'type'='hive',\n" +
                " 'default-database'='default',\n" +
                " 'hive-conf-dir'='D:/IDE/stream-dev/stream-realtime/src/main/resources/'\n"+
                ")";
        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog","default","D:/IDE/stream-dev/stream-realtime/src/main/resources/");
        tableEnv.registerCatalog("hive-catalog",hiveCatalog);
        tableEnv.useCatalog("hive-catalog");
        tableEnv.executeSql(createHiveCatalogDDL).print();
    }
}
