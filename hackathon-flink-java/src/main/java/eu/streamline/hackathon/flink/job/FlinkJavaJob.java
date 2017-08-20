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

        int seconds = 30;
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

        DataStream<Tuple3<Integer, String, Integer>> hashtagSum = tweetEvents
                .flatMap(new FlatMapFunction<TwitEvent, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public void flatMap(TwitEvent twitEvent, Collector<Tuple3<Integer, String, Integer>> collector) throws Exception {
                        for (String hashtag : Extractor.extractTags(twitEvent.text)) {
                            collector.collect(new Tuple3<Integer, String, Integer>(0, hashtag, 1));
                        }
                    }
                })
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(seconds)))
                .sum(2);


        DataStream<Tuple3<Integer, String, Integer>> mentionsSum = tweetEvents
                .flatMap(new FlatMapFunction<TwitEvent, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public void flatMap(TwitEvent twitEvent, Collector<Tuple3<Integer, String, Integer>> collector) throws Exception {
                        for (String mention : Extractor.extractMentions(twitEvent.text)) {
                            collector.collect(new Tuple3<Integer, String, Integer>(1, mention, 1));
                        }
                    }
                })
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(seconds)))
                .sum(2);

        DataStream<Tuple3<Integer, String, Integer>> union = hashtagSum.union(mentionsSum);

        Iterator<Tuple3<Integer, String, Integer>> tuple3Iterator = DataStreamUtils.collect(union);

        File file = new File("mentions.csv");
        FileWriter mentionsFileWriter = new FileWriter(file, false);
        HashMap<String,Integer> mentionsHashmap = new HashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("id,value\n");

        File fileHt = new File("hashtags.csv");
        FileWriter hashtagFileWriter = new FileWriter(fileHt, false);
        HashMap<String,Integer> hashtagHashMap = new HashMap<>();
        StringBuilder stringBuilderHt = new StringBuilder();
        stringBuilderHt.append("id,value\n");

        long startDate = System.currentTimeMillis();

        while (tuple3Iterator.hasNext()) {
            Tuple3<Integer, String, Integer> next = tuple3Iterator.next();
            long tempDate = System.currentTimeMillis();
            if (tempDate - startDate > seconds * 1000) {
                startDate = System.currentTimeMillis();

                mentionsFileWriter = new FileWriter(file, false);
                for (Map.Entry<String,Integer> entry :mentionsHashmap.entrySet()) {
                    stringBuilder.append(entry.getKey());
                    stringBuilder.append(",");
                    stringBuilder.append(entry.getValue());
                    stringBuilder.append("\n");
                }
                mentionsFileWriter.write(stringBuilder.toString());
                mentionsFileWriter.flush();
                mentionsHashmap = new HashMap<>();
                stringBuilder = new StringBuilder();
                stringBuilder.append("id,value\n");

                hashtagFileWriter = new FileWriter(fileHt, false);
                for (Map.Entry<String,Integer> entry :hashtagHashMap.entrySet()) {
                    stringBuilderHt.append(entry.getKey());
                    stringBuilderHt.append(",");
                    stringBuilderHt.append(entry.getValue());
                    stringBuilderHt.append("\n");
                }
                hashtagFileWriter.write(stringBuilderHt.toString());
                hashtagFileWriter.flush();
                hashtagHashMap = new HashMap<>();
                stringBuilderHt = new StringBuilder();
                stringBuilderHt.append("id,value\n");

                System.out.println("Files written.");
            }
            if (next.f0 == 0) {
                // ITS A HASHTAG
                if (hashtagHashMap.containsKey(next.f1)) {
                    hashtagHashMap.put(next.f1, next.f2 + hashtagHashMap.get(next.f1));
                } else  {
                    hashtagHashMap.put(next.f1, next.f2);
                }
            } else if (next.f0 == 1) {
                // ITS A MENTION
                if (mentionsHashmap.containsKey(next.f1)) {
                    mentionsHashmap.put(next.f1, next.f2 + mentionsHashmap.get(next.f1));
                } else  {
                    mentionsHashmap.put(next.f1, next.f2);
                }
            }
        }

        mentionsFileWriter.close();
        hashtagFileWriter.close();

//        try {
//            env.execute("Flink Java Twitter Analyzer");
//        } catch (Exception e) {
//            LOG.error("Failed to execute Flink job {}", e);
//        }
    }
}
