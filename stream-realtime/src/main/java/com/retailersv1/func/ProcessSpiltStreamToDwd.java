package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.retailersv1.domain.TableProcessDwd;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

/**
 * xqy
 * 2024-12-25
 */

public class ProcessSpiltStreamToDwd extends BroadcastProcessFunction<JSONObject,TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    private MapStateDescriptor<String,TableProcessDwd> mapStateDescriptor;
    private HashMap<String, TableProcessDwd> configMap =  new HashMap<>();

    public ProcessSpiltStreamToDwd(MapStateDescriptor<String, TableProcessDwd> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String querySQL = "select * from gmall_conf.table_process_dwd";
        List<TableProcessDwd> tableProcessDwds = JdbcUtils.queryList(connection, querySQL, TableProcessDwd.class, true);
        // configMap:spu_info -> TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=null)
        for (TableProcessDwd tableProcessDwd : tableProcessDwds ){
            String type = tableProcessDwd.getSourceType();
//            if ("bootstrap-insert".equals(type)){
//                type="insert";
//            }
            configMap.put(tableProcessDwd.getSourceTable()+"-"+type,tableProcessDwd);
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
    String op = jsonObject.getString("op");
    //        if ("bootstrap-insert".equals(op)){
//            op = "insert";
//        }
    String key = table+"-"+op;
        TableProcessDwd tableProcessDwd = broadcastState.get(key);
        if (tableProcessDwd==null){
            tableProcessDwd = configMap.get(key);
    }
        if (tableProcessDwd != null ){
            System.out.println("-----------------------------");
        collector.collect(Tuple2.of(jsonObject, tableProcessDwd));
    }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = tableProcessDwd.getOp();
        String sourceType = tableProcessDwd.getSourceType();
        String key = tableProcessDwd.getSourceTable()+"-"+sourceType;
        if ("d".equals(op)){
            broadcastState.remove(key);
            configMap.remove(key);

        }else {
            broadcastState.put(key,tableProcessDwd);
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
    }

}
