package com.catalog;

import com.stream.common.utils.ConfigUtils;
import com.stream.utils.HiveCatalogUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * xqy
 * 2024-12-30
 */

public class CreateHbaseDimDDLCataLog {
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String ZOOKEEPER_SERVER_HOST_LIST = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String DROP_TABEL_PREFIX = "drop table if exists ";
    private static final String createHbaseDimBaseDicDDL = "create table hbase_dim_base_dic (" +
            "    rk string," +
            "    info row<dic_name string, parent_code string>," +
            "    primary key (rk) not enforced" +
            ")" +
            "with (" +
            "    'connector' = '"+HBASE_CONNECTION_VERSION+"'," +
            "    'table-name' = '"+HBASE_NAME_SPACE+":dim_base_dic'," +
            "    'zookeeper.quorum' = '"+ZOOKEEPER_SERVER_HOST_LIST+"'" +
            ")";
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        HiveCatalog hiveCatalog = HiveCatalogUtils.getHiveCatalog("hive-catalog");
        tableEnv.registerCatalog("hive-catalog", hiveCatalog);
        tableEnv.useCatalog("hive-catalog");
        tableEnv.executeSql("show tables;").print();
        tableEnv.executeSql(DROP_TABEL_PREFIX + getCreateTableDDLTableName(createHbaseDimBaseDicDDL));
        tableEnv.executeSql("show tables;").print();
        tableEnv.executeSql(createHbaseDimBaseDicDDL).print();
        tableEnv.executeSql("show tables;").print();
        tableEnv.executeSql("select * from hbase_dim_base_dic").print();
    }
    public static String getCreateTableDDLTableName(String createDDL){
        return createDDL.split(" ")[2].trim();
    }
}
