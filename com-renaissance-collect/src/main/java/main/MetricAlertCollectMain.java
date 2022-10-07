package main;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import schema.KafkaSchema;

import java.util.Properties;

public class MetricAlertCollectMain {
    public static void main(String[] args) throws Exception {

        //kafka prop
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka-server-2.7:9092");
        kafkaProps.setProperty("zookeeper.connect", "zookeeper-server:2181");

        kafkaProps.put("max.request.size", 2147483647);
        kafkaProps.put("buffer.memory", 2147483647);
        kafkaProps.put("request.timeout.ms", 30000000);

        final OutputTag<String> memusedtag = new OutputTag<String>("mem.used") {
        };
        final OutputTag<String> cpuuseage = new OutputTag<String>("cpu.usage") {
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("openalert-mysql")
                .port(3306)
                .databaseList("bdp_metric") // set captured database
                .tableList("bdp_metric.metric") // set captured table
                .username("root")
                .password("admin123")
                .startupOptions(StartupOptions.initial())
                .deserializer(new KafkaSchema()) // converts SourceRecord to JSON String
                .build();
        // enable checkpoint
        env.enableCheckpointing(3000);
        DataStreamSource<String> metricCdcStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Metric Source");
        SingleOutputStreamOperator<String> mainStream = metricCdcStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String metric, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {

                JSONObject metricjson = (JSONObject) JSONObject.parse(metric);
                JSONObject after = (JSONObject) JSONObject.parse(metricjson.getString("after"));

                String name = null;
                if (after.containsKey("name")) {

                    name = (String) after.get("name");

                    if (name.equals("mem.used"))
                        context.output(memusedtag, after.toJSONString());
                    if (name.equals("cpu.usage"))
                        context.output(cpuuseage, after.toJSONString());

                }
                collector.collect(metric);
            }
        });

        System.out.println("sink：");
        DataStream<String> memusedStream = mainStream.getSideOutput(memusedtag);
        DataStream<String> cpuusageStream = mainStream.getSideOutput(cpuuseage);


        //sink
        FlinkKafkaProducer<String> memusedKafkaSink = new FlinkKafkaProducer<String>(
                "mem.used",
                new SimpleStringSchema()
                ,kafkaProps

        );
        FlinkKafkaProducer<String> cpuusageKafkaSink = new FlinkKafkaProducer<String>(
                "cpu.usage",
                new SimpleStringSchema()
                ,kafkaProps

        );


        memusedStream.addSink(memusedKafkaSink);
        cpuusageStream.addSink(cpuusageKafkaSink);

        memusedStream.print("memused:");
        cpuusageStream.print("cpuusage:");



        env.execute("Print MySQL Snapshot + Binlog");
    }



}
