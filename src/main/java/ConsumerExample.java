import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {

    public static void receiveMsg(String topic) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        props.put("group.id", "grouptest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            System.out.println("consumer is listening");
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, cle = %s, valeur = %s\n", record.offset(), record.key(),
                            record.value());

            }
        } finally {
            consumer.close();
        }

    }
}
