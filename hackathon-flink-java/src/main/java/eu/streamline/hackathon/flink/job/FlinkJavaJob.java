package eu.streamline.hackathon.flink.job;

import eu.streamline.hackathon.flink.operations.Extractor;
import eu.streamline.hackathon.flink.source.MyTwitterSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Read the twitter authentication data
        Properties props = new Properties();
        InputStream in = FlinkJavaJob.class.getClassLoader().getResourceAsStream("twitter.properties");
        props.load(in);
        in.close();

        // Create the data source
        DataStream<String> streamSource = env.addSource(new MyTwitterSource(props));

        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String x : Extractor.extract(s)) {
                    collector.collect(new Tuple2<String, Integer>(x,1));
                }
            }
        }).print();

        try {
            env.execute("Flink Java GDELT Analyzer");
        } catch (Exception e) {
            LOG.error("Failed to execute Flink job {}", e);
        }
    }
}
