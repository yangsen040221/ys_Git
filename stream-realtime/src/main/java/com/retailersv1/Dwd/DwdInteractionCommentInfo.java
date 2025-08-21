package com.retailersv1.Dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 杨森
 */
public class DwdInteractionCommentInfo {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.comment.info");


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_ods_ecommerce_order"));
      //  tEnv.executeSql("select * from ods_ecommerce_order where `source`['table'] = 'comment_info' ").print();

        Table commentInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "MD5(CAST(`after`['appraise'] AS STRING)) appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "ts_ms,\n" +
                "proc_time\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'comment_info'");
       // commentInfo.execute().print();

 tEnv.createTemporaryView("comment_info", commentInfo);

       // tEnv.executeSql("select * from comment_info").print();

        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " row_key string,\n" +
                " info ROW<dic_code string,dic_name STRING>,\n" +
                " PRIMARY KEY (row_key) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));
     //  tEnv.executeSql("select info.dic_code,info.dic_name from base_dic").print();


        Table joinedTable = tEnv.sqlQuery("" +
                "SELECT " +
                "  ci.id,          -- 评论ID\n" +
                "  ci.user_id,     -- 用户ID\n" +
                "  ci.sku_id,      -- 商品SKU\n" +
                "  ci.appraise,    -- 评价编码（MD5后）\n" +
                "  bd.info.dic_name AS appraise_name, -- 关联字典获取评价名称（如“好评”）\n" +
                "  ci.comment_txt, -- 评论内容\n" +
                "  ci.ts_ms,       -- 时间戳\n" +
                "  ci.proc_time    -- 处理时间\n" +
                "FROM       ci\n" +
                // 时态表关联：保证关联时字典表数据的一致性（基于评论的处理时间）
                "LEFT JOIN base_dic FOR SYSTEM_TIME AS OF ci.proc_time bd\n" +
                // 单一rowkey关联（HBase要求）：评论的预计算rowkey = 字典表的row_key
                "ON ci.appraise_rowkey = bd.row_key");







    }
}
