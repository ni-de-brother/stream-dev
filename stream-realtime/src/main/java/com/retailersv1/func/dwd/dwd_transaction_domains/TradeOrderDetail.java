package com.retailersv1.func.dwd.dwd_transaction_domains;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * xqy
 * 2024-12-27
 */
//交易域下单事务事实表 trade_order_detail 1.订单详情表 2.订单表 3.活动表 4.优惠卷表
public class TradeOrderDetail {
//    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
//    private static final String dwd_trade_order_detail = ConfigUtils.getString("dwd.trade.order.detail");
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
        /**
         * 订单详情
         */
        Table odTable = tableEnv.sqlQuery("select \n" +
                "  `after`['id'] id, \n" +
                "  `after`['order_id'] order_id, \n" +
                "  `after`['sku_id'] sku_id, \n" +
                "  `after`['sku_name'] sku_name, \n" +
                "  `after`['order_price'] order_price, \n" +
                "  `after`['sku_num'] sku_num, \n" +
                "  `after`['create_time'] create_time, \n" +
                "  `after`['split_total_amount'] split_total_amount, \n" +
                "  `after`['split_activity_amount'] split_activity_amount, \n" +
                "  `after`['split_coupon_amount'] split_coupon_amount, \n" +
                "  ts_ms\n" +
                "from topic_db\n" +
                "where source['db']='gmall'\n" +
                "and source['table']='order_detail'\n" +
                "and `op` in ('bootstrap-insert','insert','r')");
        tableEnv.createTemporaryView("order_delete_info",odTable);
//        tableEnv.sqlQuery("select * from order_detail_info").execute().print();
        /**
         * 订单表
         */
        Table oiTable = tableEnv.sqlQuery("select \n" +
                "  `after`['id'] id, \n" +
                "  `after`['user_id'] user_id, \n" +
                "  `after`['order_id'] user_id, \n" +
                "  `after`['province_id'] province_id \n" +
                "from topic_db\n" +
                "where source['db']='gmall'\n" +
                "and source['table']='order_info'\n" +
                "and `op` in ('bootstrap-insert','insert','r')");
        tableEnv.createTemporaryView("order_info", oiTable);
//        tableEnv.sqlQuery("select * from order_info limit 10").execute().print();
        /**
         * 活动表
         */
        Table odaTable = tableEnv.sqlQuery("select \n" +
                "  `after`['order_detail_id'] id, \n" +
                "  `after`['activity_id'] activity_id, \n" +
                "  `after`['activity_rule_id'] activity_rule_id,\n" +
                "  `after`['order_id'] order_id\n" +
                "from topic_db\n" +
                "where source['db']='gmall'\n" +
                "and source['table']='order_detail_activity'\n" +
                "and `op` in ('bootstrap-insert','insert','r')");
        tableEnv.createTemporaryView("order_detail_activity", odaTable);
//        tableEnv.sqlQuery("select * from order_detail_activity limit 10").execute().print();
        /**
         * 优惠卷表
         */
        Table odcTable = tableEnv.sqlQuery("select \n" +
                "  `after`['order_detail_id'] id, \n" +
                "  `after`['coupon_id'] coupon_id,\n" +
                "  `after`['order_id'] order_id\n" +
                "from topic_db\n" +
                "where source['db']='gmall'\n" +
                "and source['table']='order_detail_coupon'\n" +
                "and `op` in ('bootstrap-insert','insert','r')");
        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
   //tableEnv.sqlQuery("select * from order_detail_coupon limit 10").execute().print();

        /**
         * 关联表
         */
        Table table6 = tableEnv.sqlQuery("select\n" +
                " od.id,\n" +
                "  od.order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts_ms \n" +
                "from order_delete_info od\n" +
                " join order_info oi on od.order_id = oi.id\n" +
                " left join order_detail_activity oda on oda.order_id=oi.id\n" +
                " left join order_detail_coupon odc on odc.order_id=oi.id");
        tableEnv.createTemporaryView("xx",table6);
        tableEnv.sqlQuery("select * from xx limit 10 ").execute().print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
