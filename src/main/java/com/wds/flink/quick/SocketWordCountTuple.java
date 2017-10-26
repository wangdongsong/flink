package com.wds.flink.quick;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * 使用Tuple2<String, Integer>实现word count
 * Created by wangdongsong1229@163.com on 2017/10/26.
 */
public class SocketWordCountTuple {

    public static void main(String[] args) throws Exception {
        int port = 8080;
        String hostname = "127.0.0.1";

        try {
            final ParameterTool param = ParameterTool.fromArgs(args);
            hostname = param.has("hostname") ? param.get("hostname") : "localhost";
            port = param.has("port") ? param.getInt("port") : port;
        } catch (Exception e) {
            System.err.println("No port specified. Please run SocketWordCountTuple --port");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream(hostname, port)
                .flatMap((String sentence, Collector<Tuple2<String, Integer>> out) -> {
                    Arrays.stream(sentence.split(" ")).forEach((word) -> {out.collect(new Tuple2<>(word, 1));});})
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        dataStream.print();

        env.execute("SocketWordCountTuple");
    }

}
