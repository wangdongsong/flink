package com.wds.flink.cep;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CEP Retaller example
 *
 * Created by WANGDONGSONG846 on 2017-11-22.
 */
public class RetallerExample {

    private static final String SRC = "a";
    private static final String DST = "h";
    private static final int NUM_STOPS = 5;

    private static final Pattern<Transport, ?> pattern = Pattern.<Transport>begin("start")
            .where(new SimpleCondition<Transport>() {
                @Override
                public boolean filter(Transport value) throws Exception {
                    return Objects.equals(value.getFrom(), SRC);
                }
            })
            .followedBy("middle")
            .where(new IterativeCondition<Transport>() {
                @Override
                public boolean filter(Transport transport, Context<Transport> context) throws Exception {
                    Iterable<Transport> middleStops = context.getEventsForPattern("middle");
                    String currentLocation = middleStops.iterator().hasNext() ? getLastDestinationAndStopCountForPattern(middleStops).f0 : getLastDestinationAndStopCountForPattern(context, "start").f0;

                    return Objects.equals(transport.getFrom(), currentLocation);
                }
            }).oneOrMore().followedBy("end").where(new IterativeCondition<Transport>() {
                @Override
                public boolean filter(Transport transport, Context<Transport> context) throws Exception {
                    Tuple2<String, Integer> locationAndStopCount = getLastDestinationAndStopCountForPattern(context, "middle");
                    return locationAndStopCount.f1 >= (NUM_STOPS - 1) && Objects.equals(locationAndStopCount.f0, transport.getFrom()) && Objects.equals(transport.getTo(), DST);
                }
            }).within(Time.hours(24L));




    public static void main(String[] args) throws Exception {
        List<Transport> sampleData = new ArrayList<>();

        sampleData.add(new Transport(1, "a", "b", 0L));
        sampleData.add(new Transport(1, "b", "c", 1L));
        sampleData.add(new Transport(1, "c", "d", 2L));
        sampleData.add(new Transport(1, "d", "e", 3L));
        sampleData.add(new Transport(1, "e", "f", 4L));
        sampleData.add(new Transport(1, "f", "g", 5L));
        sampleData.add(new Transport(1, "g", "h", 6L));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Transport> keyedInput = env.fromCollection(sampleData).keyBy(element -> element.getProdId());

        CEP.pattern(keyedInput, pattern).flatSelect(new PatternFlatSelectFunction<Transport, String>() {
            @Override
            public void flatSelect(Map<String, List<Transport>> pattern, Collector<String> out) throws Exception {
                final StringBuilder stringBuilder = new StringBuilder();
                pattern.entrySet().stream().map(entry -> entry.getValue()).forEach(element -> {stringBuilder.append(element); out.collect(stringBuilder.toString());});
            }
        }).print();

        env.execute();

    }

    public static class Transport {
        private final int prodId;
        private final String from;
        private final String to;

        private final long timestamp;

        public Transport(int prodId, String from, String to, long timestamp) {
            this.prodId = prodId;
            this.from = from;
            this.to = to;
            this.timestamp = timestamp;
        }

        public int getProdId() {
            return prodId;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Transport{" + "prodId=" + prodId + ", from=" + from + ", to=" + to + "}";
        }
    }

    private static Tuple2<String, Integer> getLastDestinationAndStopCountForPattern(IterativeCondition.Context<Transport> ctx, String patternName) {
        return getLastDestinationAndStopCountForPattern(ctx.getEventsForPattern(patternName));
    }

    private static Tuple2<String, Integer> getLastDestinationAndStopCountForPattern(Iterable<Transport> events) {
        Tuple2<String, Integer> locationAndStopCount = new Tuple2<>("", 0);

        for (Transport transport : events) {
            locationAndStopCount.f0 = transport.getTo();
            locationAndStopCount.f1++;
        }
        return locationAndStopCount;
    }

}


