package com.retailersv1.Dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.Dws.comment.TradeProvinceOrderBean;
import com.retailersv1.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.DorisUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

public class DwsTradeProvinceOrderWindow {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DWD_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    @SneakyThrows
    public static void main(String[] args) {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());

        // 2. 读取Kafka数据源（dwd层订单明细主题）
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        DWD_ORDER_DETAIL,
                        new Date().toString(), // 消费者组ID（建议后续改为固定值，避免重复消费）
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "Kafka-DWD-Order-Detail-Source"
        );

        // 3. Kafka消息转换为JSON对象（过滤null消息，避免空指针）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                        // 过滤null和空字符串消息
                        if (s != null && !s.trim().isEmpty()) {
                            try {
                                JSONObject jsonObject = JSON.parseObject(s.trim());
                                // 过滤反序列化后为null的无效JSON
                                if (jsonObject != null) {
                                    collector.collect(jsonObject);
                                }
                            } catch (Exception e) {
                                // 捕获JSON解析异常，避免单条坏消息导致任务崩溃
                                System.err.println("JSON解析失败，跳过无效消息：" + s + "，异常信息：" + e.getMessage());
                            }
                        }
                    }
                }
        );

        // 4. 按订单明细ID去重（处理Kafka重复消费，基于StateTTL实现）
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 存储上一次处理的订单明细JSON（设置10秒TTL，避免State膨胀）
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> stateDesc = new ValueStateDescriptor<>(
                                "Last-Order-Detail-State",
                                JSONObject.class
                        );
                        // 配置State的TTL（10秒过期，自动清理旧数据）
                        stateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(stateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            // 重复消息处理：对金额字段取反，抵消上一次的计算结果
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        // 更新State为当前最新消息，并输出当前消息
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        // 5. 分配事件时间和水印（关键修改：去掉ts×1000，直接使用毫秒级原始ts）
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps() // 假设数据ts严格递增（非递增场景需改用forBoundedOutOfOrderness）
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                                        // 核心修改：原始ts已是毫秒级，无需×1000（避免时间戳溢出）
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        )
        );

        // 6. 转换为业务Bean（TradeProvinceOrderBean），提取省份、金额、订单ID等核心字段
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        // 提取字段（建议增加非空校验，避免后续空指针）
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts");
                        String orderId = jsonObj.getString("order_id");

                        // 构建业务Bean
                        return TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount) // 订单金额（重复消息已处理为正负抵消）
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId))) // 存储订单ID，用于统计订单数
                                .ts(ts) // 保留原始事件时间（毫秒级）
                                .build();
                    }
                }
        );

        // 7. 按省份ID分组 → 10秒滚动窗口聚合
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );

        // 8. 窗口聚合（Reduce合并金额+订单ID集合，WindowFunction补充窗口时间信息）
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                // ReduceFunction：合并同省份的订单金额和订单ID集合
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        // 合并订单金额（正负抵消重复消息的影响）
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        // 合并订单ID集合（去重，用于统计窗口内订单数）
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                // WindowFunction：补充窗口开始/结束时间、日期等维度信息
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String provinceId, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        // 补充窗口时间维度（基于窗口的毫秒级时间戳转换，格式正常）
                        String stt = DateTimeUtils.tsToDateTime(window.getStart()); // 窗口开始时间（yyyy-MM-dd HH:mm:ss）
                        String edt = DateTimeUtils.tsToDateTime(window.getEnd());   // 窗口结束时间（yyyy-MM-dd HH:mm:ss）
                        String curDate = DateTimeUtils.tsToDate(window.getStart()); // 窗口日期（yyyy-MM-dd）
                        // 统计窗口内订单数（订单ID集合的大小）
                        long orderCount = orderBean.getOrderIdSet().size();

                        // 赋值窗口维度信息并输出
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount(orderCount);
                        out.collect(orderBean);
                    }
                }
        );

        // 9. 打印聚合结果（调试用，确认时间格式和数据正确性）
        reduceDS.print("Province-Order-Window-Agg: ");

        // 10. 写入Doris（dws层省份订单窗口表）
        reduceDS
                .map(new BeanToJsonStrMapFunction<>()) // Bean转换为JSON字符串（适配Doris Sink格式）
                .sinkTo(DorisUtils.getDorisSink("dws_trade_province_order_window")) // 写入Doris表
                .name("Doris-Sink-DWS-Province-Order-Window");

        // 11. 启动Flink任务
        env.execute("DWS-Trade-Province-Order-Window-Agg-Job");
    }
}