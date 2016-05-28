import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

import com.sun.org.apache.bcel.internal.util.ClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubProducer {

    private EventHubClient ehClient;
    private TwitterStreamReader twitterStreamReader;
    private Logger logger = LoggerFactory.getLogger(EventHubProducer.class);

    EventHubProducer() throws ServiceBusException, ExecutionException, InterruptedException, IOException  {
        final String namespaceName = "skundu";
        final String eventHubName = "twitter";
        final String sasKeyName = "RootManageSharedAccessKey";
        final String sasKey = loadSasKey();

        ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
        ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());

        ArrayList<String> trackTerms = new ArrayList<String>();
        trackTerms.add("#monacogp");
        twitterStreamReader = new TwitterStreamReader(trackTerms);
    }

    public void run() throws InterruptedException, UnsupportedEncodingException, ServiceBusException {
        while(!twitterStreamReader.isDone()) {
            String tweet = twitterStreamReader.getTweet();
            if(tweet != null) {
                logger.info(tweet);
                byte[] payloadBytes = tweet.getBytes("UTF-8");

                EventData sendEvent = new EventData(payloadBytes);
                ehClient.sendSync(sendEvent);
            }
        }
        twitterStreamReader.closeConnection();
        ehClient.close();
    }

    private String loadSasKey() throws FileNotFoundException, YamlException {
        ClassLoader classLoader = new ClassLoader();
        YamlReader reader = new YamlReader(new FileReader(classLoader.getResource("azure-eventhub.yaml").getFile()));
        Map config = (Map) reader.read();
        return (String) config.get("sas-key");
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(EventHubProducer.class);
        try {
            EventHubProducer eventHubProducer = new EventHubProducer();
            eventHubProducer.run();
        } catch (Exception e) {
            logger.info("Caught exception " + e.getMessage());
        }
    }

}

