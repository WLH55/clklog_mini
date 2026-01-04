package com.zcunsoft.clklog.simple.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 简单的 Kafka 消费者
 * 从 Kafka 消费数据，暂时只打印日志，后续可以添加数据处理逻辑
 */
public class SimpleKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SimpleKafkaConsumer(String bootstrapServers, String topic, String groupId) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        this.consumer = new KafkaConsumer<>(props);
    }

    /**
     * 开始消费数据
     */
    public void start() {
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("开始消费 Kafka topic: {}", topic);

        int messageCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    try {
                        String value = record.value();
                        logger.info("收到消息 #{} (partition: {}, offset: {})",
                                messageCount, record.partition(), record.offset());

                        // 解析 JSON 并打印基本信息
                        JsonNode jsonNode = objectMapper.readTree(value);
                        String event = jsonNode.has("event") ? jsonNode.get("event").asText() : "unknown";
                        String distinctId = jsonNode.has("distinct_id") ? jsonNode.get("distinct_id").asText() : "unknown";

                        logger.info("  - event: {}, distinct_id: {}", event, distinctId);

                    } catch (Exception e) {
                        logger.error("处理消息失败: {}", record.value(), e);
                    }
                }
            }
        } finally {
            consumer.close();
            logger.info("消费者已关闭");
        }
    }

    public static void main(String[] args) {
        // 从环境变量或默认值读取配置
        String kafkaServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String kafkaTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "clklog");
        String groupId = System.getenv().getOrDefault("KAFKA_GROUP_ID", "simple-consumer-group");

        logger.info("配置信息:");
        logger.info("  Kafka Bootstrap Servers: {}", kafkaServers);
        logger.info("  Topic: {}", kafkaTopic);
        logger.info("  Group ID: {}", groupId);

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(kafkaServers, kafkaTopic, groupId);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("接收到关闭信号，正在退出...");
        }));

        consumer.start();
    }
}
