package com.retailersv1.func.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * xqy
 * 2024-12-27
 */
//交易域下单事务事实表
public class TradeOrderDetail {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_db_topic = ConfigUtils.getString("REALTIME.KAFKA.DB.TOPIC");
    public static void main(String[] args) {
        //读取kafka topic_db数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        DataStreamSource<String> streamSource = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "flink_mysql");
//        streamSource.print();
//        SingleOutputStreamOperator<JSONObject> mapStream = streamSource.map(JSONObject::parseObject)
//                .uid("to_josn")
//                        .name("to_json");
        //提取字段
//        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = mapStream.map(s -> {
//                    s.remove("transaction");
//                    JSONObject resJson = new JSONObject();
//                    if ("d".equals(s.getString("op"))){
//                        resJson.put("before",s.getJSONObject("before"));
//                    }else {
//                        resJson.put("after",s.getJSONObject("after"));
//                    }
//                    resJson.put("source",s.getString("source"));
//                    resJson.put("op",s.getString("op"));
//                    resJson.put("ts",s.getString("ts"));
//                    return resJson;
//                }).uid("clean_json_column_map")
//                .name("clean_json_column_map");
        streamSource.sinkTo(KafkaUtils.buildKafkaSink(
                kafka_botstrap_servers,
                kafka_db_topic
        )).uid("to_kafka_josn")
                .name("to_kafka_josn");
        //读取kafka并建表
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
