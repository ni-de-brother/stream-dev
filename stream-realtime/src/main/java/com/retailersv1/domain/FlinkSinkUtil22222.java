package com.retailersv1.domain;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class FlinkSinkUtil22222 {

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getDwdKafkaSink(){
        return  KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(ConfigUtils.getString("kafka.bootstrap.servers"))
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        JSONObject f0 = jsonObjectTableProcessDwdTuple2.f0;
                        TableProcessDwd f1 = jsonObjectTableProcessDwdTuple2.f1;
                        String sinkTable = f1.getSinkTable();
                        JSONObject data = f0.getJSONObject("after");
                        return new ProducerRecord<>(sinkTable, Bytes.toBytes(data.toJSONString()));
                    }
                })
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("bw-base-db"  + System.currentTimeMillis())

                // 关注一下
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();

    }


}