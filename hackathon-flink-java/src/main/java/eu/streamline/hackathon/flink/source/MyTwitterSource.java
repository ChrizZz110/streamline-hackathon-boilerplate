package eu.streamline.hackathon.flink.source;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.common.DelimitedStreamReader;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.java.ClosureCleaner;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

public class MyTwitterSource extends RichSourceFunction<String> implements StoppableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MyTwitterSource.class);
    private static final long serialVersionUID = 1L;
    public static final String CONSUMER_KEY = "twitter-source.consumerKey";
    public static final String CONSUMER_SECRET = "twitter-source.consumerSecret";
    public static final String TOKEN = "twitter-source.token";
    public static final String TOKEN_SECRET = "twitter-source.tokenSecret";
    public static final String CLIENT_NAME = "twitter-source.name";
    public static final String CLIENT_HOSTS = "twitter-source.hosts";
    public static final String CLIENT_BUFFER_SIZE = "twitter-source.bufferSize";
    private final Properties properties;
    private MyTwitterSource.EndpointInitializer initializer = new MyTwitterSource.SampleStatusesEndpoint();
    private transient BasicClient client;
    private transient Object waitLock;
    private transient boolean running = true;

    public MyTwitterSource(Properties properties) {
        checkProperty(properties, CONSUMER_KEY);
        checkProperty(properties, CONSUMER_SECRET);
        checkProperty(properties, TOKEN);
        checkProperty(properties, TOKEN_SECRET);
        this.properties = properties;
    }

    private static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    public void setCustomEndpointInitializer(MyTwitterSource.EndpointInitializer initializer) {
        Objects.requireNonNull(initializer, "Initializer has to be set");
        ClosureCleaner.ensureSerializable(initializer);
        this.initializer = initializer;
    }

    public void open(Configuration parameters) throws Exception {
        this.waitLock = new Object();
    }

    public void run(final SourceContext<String> ctx) throws Exception {
        LOG.info("Initializing Twitter Streaming API connection");
        StreamingEndpoint endpoint = this.initializer.createEndpoint();
        Authentication auth = new OAuth1(this.properties.getProperty("twitter-source.consumerKey"), this.properties.getProperty("twitter-source.consumerSecret"), this.properties.getProperty("twitter-source.token"), this.properties.getProperty("twitter-source.tokenSecret"));
        this.client = (new ClientBuilder()).name(this.properties.getProperty("twitter-source.name", "flink-twitter-source")).hosts(this.properties.getProperty("twitter-source.hosts", "https://stream.twitter.com")).endpoint(endpoint).authentication(auth).processor(new HosebirdMessageProcessor() {
            public DelimitedStreamReader reader;

            public void setup(InputStream input) {
                this.reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET, Integer.parseInt(MyTwitterSource.this.properties.getProperty("twitter-source.bufferSize", "50000")));
            }

            public boolean process() throws IOException, InterruptedException {
                String line = this.reader.readLine();
                ctx.collect(line);
                return true;
            }
        }).build();
        this.client.connect();
        this.running = true;
        LOG.info("Twitter Streaming API connection established successfully");

        while(this.running) {
            Object var4 = this.waitLock;
            synchronized(this.waitLock) {
                this.waitLock.wait(100L);
            }
        }

    }

    public void close() {
        this.running = false;
        LOG.info("Closing source");
        if (this.client != null) {
            this.client.stop();
        }

        Object var1 = this.waitLock;
        synchronized(this.waitLock) {
            this.waitLock.notify();
        }
    }

    public void cancel() {
        LOG.info("Cancelling Twitter source");
        this.close();
    }

    public void stop() {
        LOG.info("Stopping Twitter source");
        this.close();
    }

    private static class SampleStatusesEndpoint implements MyTwitterSource.EndpointInitializer, Serializable {
        private SampleStatusesEndpoint() {
        }

        public StreamingEndpoint createEndpoint() {
            StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
            endpoint.stallWarnings(false);
            endpoint.delimited(false);
            return endpoint;
        }
    }

    public interface EndpointInitializer {
        StreamingEndpoint createEndpoint();
    }
}