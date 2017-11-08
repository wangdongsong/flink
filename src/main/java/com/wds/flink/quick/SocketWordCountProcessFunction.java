package com.wds.flink.quick;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.StreamSupport;

/**
 * Created by WANGDONGSONG846 on 2017-11-06.
 */
public class SocketWordCountProcessFunction {

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
                    Arrays.stream(sentence.split(" ")).forEach((word) -> {
                        out.collect(new Tuple2<>(word, 1));
                    });
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).apply((Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) -> {

                    System.out.println("-------------start-----" + Thread.currentThread().getName() + "----------");
                    System.out.println(tuple + Thread.currentThread().getName());

                    input.forEach((t) -> System.out.println(t + Thread.currentThread().getName()));

                    Integer sum = StreamSupport.stream(input.spliterator(), false).mapToInt((Tuple2<String, Integer> in) -> in.f1).sum();
                });

        dataStream.print();

        env.execute("SocketWordCountTuple");
    }



}


