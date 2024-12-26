package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.FlinkSinkUtil22222;
import com.retailersv1.domain.TableProcessDwd;
import com.retailersv1.func.ProcessSpiltStreamToDwd;
import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.Set;

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
        //main>>>>>>:12> {"op":"r","after":{"birthday":10658,"create_time":1654646400000,"login_name":"1rm3htq","nick_name":"阿炎","name":"马炎","user_level":"1","phone_num":"13292258292","id":15,"email":"1rm3htq@qq.com"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"user_info"},"ts_ms":1735177918147}
        // main>>>>>>:9> {"op":"r","after":{"birthday":11389,"gender":"F","create_time":1654646400000,"login_name":"7bgplo7da6h2","nick_name":"勤勤","name":"南门勤","user_level":"1","phone_num":"13418647143","id":4,"email":"7bgplo7da6h2@163.com"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"user_info"},"ts_ms":1735177918146}
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
//        confStream.print();
//{"op":"r","after":{"source_type":"update","sink_table":"dwd_tool_coupon_use","source_table":"coupon_use","sink_columns":"id,coupon_id,user_id,order_id,using_time,used_time,coupon_status"}}

//     //todo 创建状态
        MapStateDescriptor<String, TableProcessDwd> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcast = tableDwdStream.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connect = cdcDbMainStreamMap.connect(broadcast);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = connect.process(new ProcessSpiltStreamToDwd(mapStageDesc));
        // Received tuple: ({"op":"r","after":{"birthday":14104,"gender":"F","create_time":1731537465000,"login_name":"r06e353","nick_name":"梅梅","name":"雷梅","user_level":"3","phone_num":"13237869752","id":248,"email":"r06e353@3721.net"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"user_info"},"ts_ms":1735184432253},TableProcessDwd(sourceTable=user_info, sourceType=insert, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r))
        // Received tuple: ({"op":"r","after":{"birthday":12704,"gender":"M","create_time":1731512439000,"login_name":"lynymbk","nick_name":"阿友","name":"夏侯友","user_level":"1","phone_num":"13319274811","id":224,"email":"lynymbk@3721.net"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"user_info"},"ts_ms":1735184432252},TableProcessDwd(sourceTable=user_info, sourceType=insert, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r))
//过滤字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2) throws Exception {
                JSONObject f0 = jsonObjectTableProcessDwdTuple2.f0;
                TableProcessDwd f1 = jsonObjectTableProcessDwdTuple2.f1;
                JSONObject data = f0.getJSONObject("after");
                String sinkColumns = f1.getSinkColumns();
                Set<String> keys = data.keySet();
                keys.removeIf(key -> !sinkColumns.contains(key));
                return jsonObjectTableProcessDwdTuple2;
            }
        });
        dataStream.sinkTo(FlinkSinkUtil22222.getDwdKafkaSink());
        env.execute();
    }
}