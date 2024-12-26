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
           // {favor_info-insert=TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=null), coupon_use-update=TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=null), user_info-insert=TableProcessDwd(sourceTable=user_info, sourceType=insert, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=null), coupon_use-insert=TableProcessDwd(sourceTable=coupon_use, sourceType=insert, sinkTable=dwd_tool_coupon_get, sinkColumns=id,coupon_id,user_id,get_time,coupon_status, op=null)}
            //{favor_info-insert=TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=null), coupon_use-update=TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=null), coupon_use-insert=TableProcessDwd(sourceTable=coupon_use, sourceType=insert, sinkTable=dwd_tool_coupon_get, sinkColumns=id,coupon_id,user_id,get_time,coupon_status, op=null)}
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
//        System.out.println(broadcastState+"xxxx");
        //HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@6a2f6f80, valueSerializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@47edfd50, assignmentMode=BROADCAST}, backingMap={favor_info-insert=TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r), user_info-insert=TableProcessDwd(sourceTable=user_info, sourceType=insert, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r), coupon_use-update=TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=r), coupon_use-insert=TableProcessDwd(sourceTable=coupon_use, sourceType=insert, sinkTable=dwd_tool_coupon_get, sinkColumns=id,coupon_id,user_id,get_time,coupon_status, op=r)}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@23ac7dd0}
        //HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@6a2f6f80, valueSerializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@47edfd50, assignmentMode=BROADCAST}, backingMap={favor_info-insert=TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r), user_info-insert=TableProcessDwd(sourceTable=user_info, sourceType=insert, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r), coupon_use-update=TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=r), coupon_use-insert=TableProcessDwd(sourceTable=coupon_use, sourceType=insert, sinkTable=dwd_tool_coupon_get, sinkColumns=id,coupon_id,user_id,get_time,coupon_status, op=r)}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@23ac7dd0}
        String table = jsonObject.getJSONObject("source").getString("table");
    String op = jsonObject.getString("op");
//        System.out.println(table+":"+op);
    //        if ("bootstrap-insert".equals(op)){
//            op = "insert";
//        }
        if ("r".equals(op)){
            op = "insert";
        }
        if ("u".equals(op)){
            op = "update";
        }
    String key = table+"-"+op;
        TableProcessDwd tableProcessDwd = broadcastState.get(key);
        if (tableProcessDwd==null){
            tableProcessDwd = configMap.get(key);
    }
        if (tableProcessDwd != null ){
       //     TableProcessDwd(sourceTable=user_info, sourceType=insert, sinkTable=dwd_user_register, sinkColumns=id,create_time, op=r)
                    collector.collect(Tuple2.of(jsonObject, tableProcessDwd));
    }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        // TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=r)
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = tableProcessDwd.getOp();
        String sourceType = tableProcessDwd.getSourceType();
        String key = tableProcessDwd.getSourceTable()+"-"+sourceType;
//     System.out.println(key+"---------------------");
       // coupon_use-update
       // user_info-insert

        if ("d".equals(op)){
            broadcastState.remove(key);
            configMap.remove(key);

        }else {
            broadcastState.put(key,tableProcessDwd);
//            System.out.println(broadcastState+"broadcastState");
           // HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@6a2f6f80, valueSerializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@47edfd50, assignmentMode=BROADCAST}, backingMap={favor_info-insert=TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r), coupon_use-update=TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=r), coupon_use-insert=TableProcessDwd(sourceTable=coupon_use, sourceType=insert, sinkTable=dwd_tool_coupon_get, sinkColumns=id,coupon_id,user_id,get_time,coupon_status, op=r)}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@23ac7dd0}
           // HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@6a2f6f80, valueSerializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@47edfd50, assignmentMode=BROADCAST}, backingMap={favor_info-insert=TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r), coupon_use-update=TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupon_status, op=r), coupon_use-insert=TableProcessDwd(sourceTable=coupon_use, sourceType=insert, sinkTable=dwd_tool_coupon_get, sinkColumns=id,coupon_id,user_id,get_time,coupon_status, op=r)}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@23ac7dd0}broadcastState

        }
    }
    @Override
    public void close() throws Exception {
        super.close();
    }

}
