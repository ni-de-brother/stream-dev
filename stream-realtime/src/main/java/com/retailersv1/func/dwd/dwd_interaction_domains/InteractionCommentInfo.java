package com.retailersv1.func.dwd.dwd_interaction_domains;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * xqy
 * 2024-12-28
 */
//  互动域 评论事务事实表  comment_info（商品评论表） 读取hbase上的gmall:dim_base_dic  关联
public class InteractionCommentInfo {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String dwd_interaction_comment_info = ConfigUtils.getString("dwd_interaction_comment_info");
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `before` Map<string,string>,\n" +
                "  `after` Map<string,string>,\n" +
                "  `source` Map<string,string>,\n" +
                "  `op` STRING,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time as proctime(),\n" +
                "  `transaction` STRING,\n" +
                " row_time as TO_TIMESTAMP_LTZ(ts_ms*1000,3),\n" +
                " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "  'properties.group.id' = 'your_consumer_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        //读取hbase表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '" + CDH_ZOOKEEPER_SERVER + "'\n" +
                ")");
//        tableEnv.executeSql("select * from base_dic limit 10").print();
        //读取comment_info（商品评价表）
        Table table = tableEnv.sqlQuery("select " +
                " `after`['id'] id," +
                " `after`['user_id'] user_id," +
                " `after`['nick_name'] nick_name," +
                " `after`['sku_id'] sku_id," +
                " `after`['spu_id'] spu_id," +
                " `after`['order_id'] order_id," +
                " `after`['appraise'] appraise," +
                " `after`['comment_txt'] comment_txt," +
                " `after`['create_time'] create_time," +
                " proc_time " +
                " from topic_db " +
                " where source['db']='gmall' " +
                " and source['table']='comment_info' " +
                " and `op` in ('r') ");
        tableEnv.createTemporaryView("comment_info",table);
        //关联表
        Table table1 = tableEnv.sqlQuery("select " +
                " id," +
                " user_id," +
                " nick_name," +
                " sku_id," +
                " spu_id," +
                " order_id," +
                " appraise," +
                " info.dic_name," +
                " comment_txt," +
                " create_time " +
                " from comment_info c " +
                " join base_dic for system_time as of c.proc_time as b " +
                " on c.appraise = b.rowkey ");
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table1);
        SingleOutputStreamOperator<String> map = rowDataStream.map(String::valueOf);
        map.sinkTo(KafkaUtils.buildKafkaSink(
                 kafka_bootstrap_servers,
                dwd_interaction_comment_info
        )).uid("dwd_interaction_comment_info")
                .name("dwd_interaction_comment_info");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
