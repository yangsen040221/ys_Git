package com.retailersv1.Dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description:评价事实表
 */

public class DbusDwdCommentAndDic {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.comment.info");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE ods_data_kafka_topic (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "test"));
//        tableEnv.executeSql("select * from ods_data_kafka_topic where `source`['table'] = 'comment_info' ").print();
        Table commentInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "MD5(CAST(`after`['appraise'] AS STRING)) appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "ts_ms,\n" +
                "proc_time\n" +
                "from ods_data_kafka_topic\n" +
                "where `source`['table'] = 'comment_info'");
//        commentInfo.execute().print();

        tEnv.createTemporaryView("comment_info", commentInfo);


        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();


        Table joinTable = tEnv.sqlQuery("SELECT id,\n" +
                "      user_id,\n" +
                "      sku_id,\n" +
                "      appraise,\n" +
                "      dic.info.dic_name appraise_name,\n" +
                "      comment_txt,\n" +
                "      ts_ms\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code");
//        joinTable.execute().print();


        tEnv.executeSql("CREATE TABLE "+DWD_COMMENT_INFO+" (\n" +
                "      id String,\n" +
                "      user_id string,\n" +
                "      sku_id string,\n" +
                "      appraise CHAR(32),\n" +
                "      appraise_name string,\n" +
                "      comment_txt string,\n" +
                "      ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SqlUtil.getUpsertKafkaDDL(DWD_COMMENT_INFO));


        joinTable.executeInsert(DWD_COMMENT_INFO);
    }
}
