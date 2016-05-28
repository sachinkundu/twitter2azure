import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamReader {

    private Client client;
    private BlockingQueue<String> queue;

    TwitterStreamReader(ArrayList<String> trackTerms) throws FileNotFoundException, YamlException {
        ClassLoader classLoader = getClass().getClassLoader();
        YamlReader reader = new YamlReader(new FileReader(classLoader.getResource("twitter.yaml").getFile()));
        Map config = (Map) reader.read();
        queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(trackTerms);

        Authentication auth = new OAuth1((String) config.get("consumer_key"),
                (String) config.get("consumer_secret"),
                (String) config.get("token"),
                (String) config.get("secret"));

        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();
    }

    public void closeConnection() {
        if(client.isDone()) {
            client.stop();
            System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
        }
    }

    public String getTweet() throws InterruptedException {
        if(!client.isDone()) {
            return queue.take();
        } else {
            System.out.println("Client connection closed unexpectedly: ");
            return null;
        }
    }

    public boolean isDone() {
        return client.isDone();
    }
}