import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerExample {

    public static void sendMsg(String topic, String message) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String key = topic +"_key";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        Future<RecordMetadata> f =  producer.send(record);
        System.out.println("producer has send message "+message);
        producer.close();
    }
}
