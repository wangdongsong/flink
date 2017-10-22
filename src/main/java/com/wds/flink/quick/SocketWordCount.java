package com.wds.flink.quick;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * Socket Word Count
 * Created by wangdongsong1229@163.com on 2017/10/21.
 */
public class SocketWordCount {
    private static int port = 8080;

    public static void main(String[] args) throws Exception {
        final ParameterTool param = ParameterTool.fromArgs(args);
        port = param.getInt("port");
        System.out.println(port);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                return new WordWithCount(value1.word, value1.count + value2.count);
            }
        });

        /*
        DataStream<WordWithCount> windowCounts = text.flatMap((String value, Collector<WordWithCount> out) -> {
                for (String word : value.split("\\s")) {
                    out.collect(new WordWithCount(word, 1L));
                }})
                .flatMap((String value, Collector<WordWithCount> out) -> Stream.of(value.split("\\s")).forEach((word) -> {
                        out.collect(new WordWithCount(word, 1L));
                }))
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))//.sum("word");
                .reduce((WordWithCount a, WordWithCount b) -> {return new WordWithCount(a.word, a.count + b.count);});
          */

        windowCounts.print().setParallelism(1);

        env.execute("My Socket WindowWordCount");

    }

    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }

}
