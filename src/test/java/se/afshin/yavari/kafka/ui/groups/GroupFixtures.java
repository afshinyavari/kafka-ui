package se.afshin.yavari.kafka.ui.groups;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import se.afshin.yavari.kafka.ui.kafka.KafkaClients;
import se.afshin.yavari.kafka.ui.security.Credentials;

/** Test helpers: topics with data, and group members that commit offsets. */
final class GroupFixtures {
    private GroupFixtures() {
    }

    static void topicWith(KafkaClients clients, String topic, int messages) throws Exception {
        var creds = new Credentials.Mtls();
        try {
            clients.admin(creds).createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
        try (var producer = clients.newProducer(creds)) {
            for (int i = 0; i < messages; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null, ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }
    }

    /** A real group member (subscribed, so the group is active while open). */
    static KafkaConsumer<byte[], byte[]> member(String group, String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = new KafkaConsumer<byte[], byte[]>(p);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    /** Joins the group, then commits {@code offset} for partition 0. Returns the still-open member when asked. */
    static KafkaConsumer<byte[], byte[]> commit(String group, String topic, long offset, boolean keepOpen) {
        var consumer = member(group, topic);
        long deadline = System.currentTimeMillis() + 30_000;
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200));
        }
        consumer.commitSync(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(offset)));
        if (keepOpen) {
            return consumer;
        }
        consumer.close();
        return null;
    }

    /** Waits until the broker reports the group empty (a closed member leaves immediately, but be safe). */
    static void awaitEmpty(KafkaClients clients, String group) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            var d = clients.admin(new Credentials.Mtls()).describeConsumerGroups(List.of(group)).all().get().get(group);
            if (d.members().isEmpty()) {
                return;
            }
            Thread.sleep(200);
        }
    }
}
