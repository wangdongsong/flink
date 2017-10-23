package com.wds.flink.quick;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by WANGDONGSONG846 on 2017-10-23.
 */
public class SocketWordCountLambda {
    private static int port = 8080;

    public static void main(String[] args) throws Exception {

        try {
            final ParameterTool param = ParameterTool.fromArgs(args);
            port = param.getInt("port");
            System.out.println(port);
        } catch (Exception e) {
            System.err.println("No port specified. Please run SocketWithWordCountLambda --port");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        DataStream<SocketWordCount.WordWithCount> windowCounts = text
                .map(s -> s.split("\\s"))
                .flatMap((String[] strs, Collector<SocketWordCount.WordWithCount> out) ->{ Arrays.stream(strs).forEach(str -> out.collect(new SocketWordCount.WordWithCount(str, 1L)));})
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .sum("word");

        windowCounts.print().setParallelism(1);

        env.execute("My Socket WindowWordCount");
    }

}
