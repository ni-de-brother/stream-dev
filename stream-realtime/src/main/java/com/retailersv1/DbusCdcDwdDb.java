package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDwd;
import com.retailersv1.func.ProcessSpiltStreamToDwd;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDim;
import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * xqy
 * 2024-12-25
 */

public class DbusCdcDwdDb {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    @SneakyThrows
    public static void main(String[] args) {
        CommonUtils.printCheckPropEnv(
              false,
                CDH_ZOOKEEPER_SERVER,
                CDH_HBASE_NAME_SPACE
        );
//todo 读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

// todo CDC读取；配置表
        MySqlSource<String> mySQLCdcDwdConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall_conf.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDwdStream = env.fromSource(mySQLCdcDwdConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("dwd_zl_convert_json")
                .name("dwd_zl_convert_json")
                .setParallelism(1);
        SingleOutputStreamOperator<JSONObject> cdcDbDwdStreamMap = cdcDbDwdStream.map(JSONObject::parseObject)
                .uid("dwd_cl_convert_json")
                .name("dwd_cl_convert_json")
                .setParallelism(1);
//        cdcDbMainStreamMap.print("main>>>>>>");
//        cdcDbDwdStreamMap.print("dwd>>>>>>>");
//todo 提取配置表的数据
        SingleOutputStreamOperator<TableProcessDwd> tableDwdStream = cdcDbDwdStreamMap.flatMap(new FlatMapFunction<JSONObject, TableProcessDwd>() {
            @Override
            public void flatMap(JSONObject jsonObject, Collector<TableProcessDwd> collector) throws Exception {
                TableProcessDwd tableProcessDwd;
                String op = jsonObject.getString("op");
                if ("d".equals(op)) {
                    tableProcessDwd = JSON.parseObject(jsonObject.getString("before"), TableProcessDwd.class);
                } else {
                    tableProcessDwd = JSON.parseObject(jsonObject.getString("after"), TableProcessDwd.class);
                }
                tableProcessDwd.setOp(op);
                collector.collect(tableProcessDwd);
            }
        });
//        SingleOutputStreamOperator<JSONObject> confStream = cdcDbDwdStreamMap.map(s -> {
//                    s.remove("source");
//                    s.remove("transaction");
//                    JSONObject resJson = new JSONObject();
//                    if ("d".equals(s.getString("op"))) {
//                        resJson.put("before", s.getJSONObject("before"));
//                    } else {
//                        resJson.put("after", s.getJSONObject("after"));
//                    }
//                    resJson.put("op", s.getString("op"));
//                    return resJson;
//                }).uid("clean_gmall_conf")
//                .name("clean_gmall_conf");
//        confStream.print();
//{"op":"r","after":{"source_type":"update","sink_table":"dwd_tool_coupon_use","source_table":"coupon_use","sink_columns":"id,coupon_id,user_id,order_id,using_time,used_time,coupon_status"}}

//     //todo 创建状态
        MapStateDescriptor<String, TableProcessDwd> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcast = tableDwdStream.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connect = cdcDbMainStreamMap.connect(broadcast);
        connect.process(new ProcessSpiltStreamToDwd(mapStageDesc));
        env.execute();
    }
}