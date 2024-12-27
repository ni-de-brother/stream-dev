package com.retailersv1.func.dwd.dwd_transaction_domains;

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
 * 2024-12-27
 */

public class TradeCartAdd {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String dwd_trade_cart_add = ConfigUtils.getString("dwd.trade.cart.add");
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `before` Map<string,string>,\n" +
                "  `after` Map<string,string>,\n" +
                "  `source` Map<string,string>,\n" +
                "  `op` STRING,\n" +
                "  `ts_ms` BIGINT,\n" +
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
//        tableEnv.sqlQuery("select * from topic_db where source['table']='cart_info'").execute().print();
        //交易域加购事务事实表trade_cart_add
        Table table = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['user_id'] user_id, " +
                " `after`['sku_id'] sku_id, " +
                " `after`['cart_price'] cart_price, " +
                "  if(`op` in ('bootstrap-insert','insert','r'),cast(`after`['sku_num'] as bigint),cast(`after`['sku_num'] as bigint)-cast(`before`['sku_num'] as bigint)) sku_num, " +
                " `after`['sku_name'] sku_name, " +
                " `after`['is_checked'] is_checked, " +
                " `after`['create_time'] create_time, " +
                " `after`['operate_time'] operate_time, " +
                " `after`['is_ordered'] is_ordered, " +
                " `after`['order_time'] order_time," +
                "  ts_ms " +
                " from topic_db " +
                " where source['db']='gmall' " +
                " and source['table']='cart_info' " +
                " and (`op` in ('bootstrap-insert','insert','r') or (`op`='u' and `before`['sku_num'] is not null " +
                " and cast(`after`['sku_num'] as bigint) > cast(`before`['sku_num'] as bigint) ) ) ");
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
        SingleOutputStreamOperator<String> map_trade_cart_add = rowDataStream.map(String::valueOf);
        map_trade_cart_add.sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,dwd_trade_cart_add))
                .uid("dwd_trade_cart_add")
                .name("dwd_trade_cart_add");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
