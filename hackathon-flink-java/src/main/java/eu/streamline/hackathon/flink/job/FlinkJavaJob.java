package eu.streamline.hackathon.flink.job;

import eu.streamline.hackathon.common.data.TwitEvent;
import eu.streamline.hackathon.flink.operations.Extractor;
import eu.streamline.hackathon.flink.source.MyTwitterSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Flink Java Job class
 */
public class FlinkJavaJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJavaJob.class);

    /**
     * Main application file to run the app
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws IOException {

        ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Read the twitter authentication data
        Properties props = new Properties();
        InputStream in = FlinkJavaJob.class.getClassLoader().getResourceAsStream("twitter.properties");
        props.load(in);
        in.close();

        // Create the data source
        DataStream<String> streamSource = env.addSource(new MyTwitterSource(props));

        DataStream<TwitEvent> tweetEvents = streamSource
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("{\"created");
                    }
                })
                .map(new MapFunction<String, TwitEvent>() {
                    @Override
                    public TwitEvent map(String s) throws Exception {
                        return TwitEvent.fromString(s);
                    }
                });

        DataStream<Tuple2<String, Integer>> hashtagSum = tweetEvents
                .flatMap(new FlatMapFunction<TwitEvent, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(TwitEvent twitEvent, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String hashtag : Extractor.extractTags(twitEvent.text)) {
                            collector.collect(new Tuple2<String, Integer>(hashtag, 1));
                        }
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        Iterator<Tuple2<String, Integer>> tuple2Iterator = DataStreamUtils.collect(hashtagSum);

        File fileHt = new File("hashtags.csv");
        FileWriter fileWriterHt = new FileWriter(fileHt, false);
        StringBuilder stringBuilderHt = new StringBuilder();
        stringBuilderHt.append("id,value\n");

        long startDateHt = System.currentTimeMillis();

        while (tuple2Iterator.hasNext()) {
            Tuple2<String, Integer> next = tuple2Iterator.next();
            long tempDate = System.currentTimeMillis();
            if (tempDate - startDateHt > 10000) {
                startDateHt = System.currentTimeMillis();
                fileWriterHt = new FileWriter(fileHt, false);
                fileWriterHt.write(stringBuilderHt.toString());
                fileWriterHt.flush();
                stringBuilderHt = new StringBuilder();
                stringBuilderHt.append("id,value\n");
            }
            stringBuilderHt.append(next.f0);
            stringBuilderHt.append(",");
            stringBuilderHt.append(next.f1);
            stringBuilderHt.append("\n");

        }
        fileWriterHt.close();


        DataStream<Tuple3<Integer, String, Integer>> sum = tweetEvents
                .flatMap(new FlatMapFunction<TwitEvent, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public void flatMap(TwitEvent twitEvent, Collector<Tuple3<Integer, String, Integer>> collector) throws Exception {
                        for (String mention : Extractor.extractMentions(twitEvent.text)) {
                            collector.collect(new Tuple3<Integer, String, Integer>(0, mention, 1));
                        }
                    }
                })
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(2);

        Iterator<Tuple3<Integer, String, Integer>> tuple3Iterator = DataStreamUtils.collect(sum);

        File file = new File("mentions.csv");
        FileWriter fileWriter = new FileWriter(file, false);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("id,value\n");


        long startDate = System.currentTimeMillis();

        while (tuple3Iterator.hasNext()) {
            Tuple3<Integer, String, Integer> next = tuple3Iterator.next();
            long tempDate = System.currentTimeMillis();
            if (tempDate - startDate > 10000) {
                System.out.println("Mentions written.");
                startDate = System.currentTimeMillis();
                fileWriter = new FileWriter(file, false);
                fileWriter.write(stringBuilder.toString());
                fileWriter.flush();
                stringBuilder = new StringBuilder();
                stringBuilder.append("id,value\n");
            }
            stringBuilder.append(next.f1);
            stringBuilder.append(",");
            stringBuilder.append(next.f2);
            stringBuilder.append("\n");

        }
        fileWriter.close();

//        try {
//            env.execute("Flink Java Twitter Analyzer");
//        } catch (Exception e) {
//            LOG.error("Failed to execute Flink job {}", e);
//        }
    }
}
