package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.FlinkSinkUtil;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.codehaus.stax2.validation.Validatable;

import javax.swing.text.DateFormatter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

/**
 * @Author xqy
 * @CreateTime 2024-12-24
 */

public class DbusCdcDwdLog {
    public static void main(String[] args){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        //String server, String groupId, String offset
        DataStreamSource<String> kafkaSource = env.fromSource(KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC"),
                        new Date().toString(),
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "kafkaSource");
        //5> {"common":{"ar":"8","ba":"vivo","ch":"oppo","is_new":"0","md":"vivo x90","mid":"mid_135","os":"Android 12.0","sid":"b5fb30f4-dff6-4b55-a517-01df29facd9d","uid":"203","vc":"v2.1.134"},"page":{"during_time":13010,"item":"554","item_type":"order_id","last_page_id":"order","page_id":"payment"},"ts":1731510150272}

//        kafkaSource.print();

      //  数据清洗
        SingleOutputStreamOperator<JSONObject> streamOperator = kafkaSource.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String ts = jsonObject.getString("ts");
                JSONObject common = jsonObject.getJSONObject("common");
                String mid = common.getString("mid");
                try{
                    if (ts.length()>0 && mid.length()>0){
                        collector.collect(jsonObject);
                    }
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
        });
       // streamOperator.print();
//        //添加水位线，解决数据乱序问题，并分组，使同一mid层
        KeyedStream<JSONObject, String> KeyByStream = streamOperator.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
       // 8> {"common":{"ar":"26","uid":"198","os":"Android 13.0","ch":"wandoujia","is_new":"0","md":"Redmi k50","mid":"mid_468","vc":"v2.1.132","ba":"Redmi","sid":"04fe741d-b1f0-4ae1-b1f2-213823a4489b"},"page":{"page_id":"payment","item":"543","during_time":16361,"item_type":"order_id","last_page_id":"order"},"ts":1731511536924}

//        KeyByStream.print();
        //新老用户校验
        SingleOutputStreamOperator<JSONObject> mapStream = KeyByStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                //new ValueStateDescriptor<>("my_state", String.class) 创建状态表述器 my_state状态名称 String.class状态存储的类型
                state = getRuntimeContext().getState(new ValueStateDescriptor<>("my_state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                String is_new = common.getString("is_new");
                Long ts = jsonObject.getLong("ts");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String toDate = sdf.format(new Date(ts));
                String value = state.value();
                if ("1".equals(is_new)) {
                    if (null == value) {
                        state.update(toDate);
                    } else if (!toDate.equals(value)) {
                        common.put("is_new", 0);
                    }
                } else if ("0".equals(is_new)) {
                    ts = ts - 24 * 60 * 60 * 1000;
                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String toDate2 = sdf2.format(new Date(ts));
                    state.update(toDate2);
                }
                return jsonObject;
            }
        });
       // 8> {"common":{"ar":"25","uid":"38","os":"iOS 13.2.3","ch":"Appstore","is_new":0,"md":"iPhone 14 Plus","mid":"mid_400","vc":"v2.1.134","ba":"iPhone","sid":"a5d6cc53-caf4-4c82-afab-247c75f4ea68"},"page":{"from_pos_seq":0,"page_id":"good_detail","item":"26","during_time":17207,"item_type":"sku_id","last_page_id":"home","from_pos_id":9},"displays":[{"pos_seq":0,"item":"30","item_type":"sku_id","pos_id":4},{"pos_seq":1,"item":"30","item_type":"sku_id","pos_id":4}],"actions":[{"item":"26","action_id":"favor_add","item_type":"sku_id","ts":1731511881499}],"ts":1731511879499}
//        mapStream.print();
//        //分流
        OutputTag<String> err_log = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> start_log = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> display_log = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> action_log = new OutputTag<>("action", TypeInformation.of(String.class));
        SingleOutputStreamOperator<String> processFl = mapStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //错误日志信息
                JSONObject err = jsonObject.getJSONObject("err");
                //错误日志如果存在
                if (err != null) {
                    //存入错误测流
                    context.output(err_log, jsonObject.toJSONString());
                    //从数据中删除错误日志信息
                    jsonObject.remove("err");
                }
                //获取common
                JSONObject common = jsonObject.getJSONObject("common");
                //获取启动日志信息
                JSONObject start = jsonObject.getJSONObject("start");
                //获取页面日志信息
                JSONObject page = jsonObject.getJSONObject("page");
                //获取时间戳
                Long ts = jsonObject.getLong("ts");
                //判断是否是启动日志

                if (start != null) {
                    //存入启动测流
                    context.output(start_log, jsonObject.toJSONString());
                    //从日志数据删除启动日志
                    jsonObject.remove("start");
                    //当 start 字段为 null 且 page 字段不为 null 时，说明这条日志数据主要包含页面相关的信息，进入这个分支进行处理。
                } else if (page != null) {
                    //当页面日志不为空时将不同的日志信息放入在不同的流中
                    //获取曝光日志
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            displaysJSONObject.put("page", page);
                            displaysJSONObject.put("common", common);
                            displaysJSONObject.put("ts", ts);
                            context.output(display_log, displaysJSONObject.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }
                    //获取活动日志
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page", page);
                            action.put("common", common);
                            action.put("ts", ts);
                            context.output(action_log, action.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                }
                collector.collect(jsonObject.toJSONString());
            }
        });
//    //取出测流数据
        SideOutputDataStream<String> err_log_sideOutput = processFl.getSideOutput(err_log);
        SideOutputDataStream<String> start_log_sideOutput = processFl.getSideOutput(start_log);
        SideOutputDataStream<String> display_log_sideOutput = processFl.getSideOutput(display_log);
        SideOutputDataStream<String> action_log_sideOutput = processFl.getSideOutput(action_log);

       err_log_sideOutput.print("err_log>>>>");
        //err_log>>>>:11> {"common":{"ar":"12","uid":"180","os":"Android 12.0","ch":"vivo","is_new":0,"md":"xiaomi 12 ultra ","mid":"mid_58","vc":"v2.1.132","ba":"xiaomi","sid":"3258754e-5566-4350-aae1-de64dc0a6055"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.AppError.main(AppError.java:xxxxxx)","error_code":1057},"page":{"from_pos_seq":2,"page_id":"good_detail","item":"29","during_time":13717,"item_type":"sku_id","last_page_id":"cart","from_pos_id":5},"displays":[{"pos_seq":0,"item":"8","item_type":"sku_id","pos_id":4},{"pos_seq":1,"item":"10","item_type":"sku_id","pos_id":4},{"pos_seq":2,"item":"1","item_type":"sku_id","pos_id":4},{"pos_seq":3,"item":"24","item_type":"sku_id","pos_id":4},{"pos_seq":4,"item":"35","item_type":"sku_id","pos_id":4},{"pos_seq":5,"item":"9","item_type":"sku_id","pos_id":4}],"actions":[{"item":"29","action_id":"favor_add","item_type":"sku_id","ts":1731470900880},{"item":"29","action_id":"cart_add","item_type":"sku_id","ts":1731470903880}],"ts":1731470898880}
        err_log_sideOutput.sinkTo(
                KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"dwd_err_log")
        )
        .uid("err_log_sideOutput")
        .name("err_log_sideOutput");
//      start_log_sideOutput.print("start_log>>>>");
      //start_log>>>>:5> {"common":{"ar":"19","uid":"48","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 13","mid":"mid_173","vc":"v2.1.134","ba":"iPhone","sid":"2ac30fb4-1cee-44e5-89b2-0d0d84ffdca4"},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":2983,"loading_time":4221,"open_ad_id":18},"ts":1731509064178}
//        start_log_sideOutput.sinkTo(
//                        KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"dwd_start_log")
//                )
//                .uid("start_log_sideOutput")
//                .name("start_log_sideOutput");
       // display_log_sideOutput.print("display_log>>>>");
        //display_log>>>>:7> {"pos_seq":19,"item":"15","common":{"ar":"29","uid":"59","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"Redmi k50","mid":"mid_128","vc":"v2.1.134","ba":"Redmi","sid":"f8d47490-a5c3-4d32-b661-9baf18956320"},"item_type":"sku_id","pos_id":2,"page":{"page_id":"home","refer_id":"3","during_time":8218},"ts":1731502096692}
//        display_log_sideOutput.sinkTo(
//                        KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"dwd_display_log")
//                )
//                .uid("display_log_sideOutput")
//                .name("display_log_sideOutput");

//        action_log_sideOutput.print("action_log>>>>");
       // action_log>>>>:1> {"item":"2","common":{"ar":"25","uid":"247","os":"iOS 13.2.3","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_5","vc":"v2.1.134","ba":"iPhone","sid":"e3d78065-3772-4a78-92ba-782cbe668abe"},"action_id":"get_coupon","item_type":"coupon_id","page":{"from_pos_seq":0,"page_id":"good_detail","item":"29","during_time":5263,"item_type":"sku_id","last_page_id":"good_detail","from_pos_id":4},"ts":1731508211828}

        action_log_sideOutput.sinkTo(
                        KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"dwd_action_log")
                )
                .uid("action_log_sideOutput")
                .name("action_log_sideOutput");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}