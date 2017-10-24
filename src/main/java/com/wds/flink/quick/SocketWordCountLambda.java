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


    public static void main(String[] args) throws Exception {

        int port = 8080;
        String hostname = "127.0.0.1";

        try {
            final ParameterTool param = ParameterTool.fromArgs(args);
            hostname = param.has("hostname") ? param.get("hostname") : "localhost";
            port = param.has("port") ? param.getInt("port") : port;
            System.out.println(port);
        } catch (Exception e) {
            System.err.println("No port specified. Please run SocketWithWordCountLambda --port");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        DataStream<SocketWordCount.WordWithCount> windowCounts = text
                .map(s -> s.split("\\s"))
                .flatMap((String[] strs, Collector<SocketWordCount.WordWithCount> out) ->{ Arrays.stream(strs).forEach(str -> out.collect(new SocketWordCount.WordWithCount(str, 1L)));})
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .sum("count");

        windowCounts.print().setParallelism(1);

        env.execute("My Socket WindowWordCount");
    }

}
