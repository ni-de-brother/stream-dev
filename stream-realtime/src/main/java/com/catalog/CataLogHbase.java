package com.catalog;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * xqy
 * 2024-12-30
 */

public class CataLogHbase {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog","default","D:/IDE/stream-dev/stream-realtime/src/main/resources");
        tableEnv.registerCatalog("hive-catalog",hiveCatalog);
        tableEnv.useCatalog("hive-catalog");
        tableEnv.executeSql("select rk,info.dic_name as dic_name,\n"+
                "info.dic_name as dic_name,\n"+
                "info.parent_code as parent_code\n"+
                "form hbase_dim_base_dic").print();
    }
}
