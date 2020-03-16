package org.lyming.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName KafkaConsumerDemo
 * @Description TODO
 * @Author lyming
 * @Date 2020/3/3 11:33 下午
 **/
public class KafkaConsumerDemo extends Thread {

    private final KafkaConsumer kafkaConsumer;

    public KafkaConsumerDemo(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.2.201:9092,192.168.2.202:9092,192.168.2.203:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId02");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer = new KafkaConsumer<Integer,String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(1000);
            for (ConsumerRecord record : consumerRecords) {
                System.out.println("message received:" + record.value()+":"+record.offset());
                // 提交最后一个返回的偏移量
//                kafkaConsumer.commitAsync();
            }
        }

    }

    public static void main(String[] args) {
        new KafkaConsumerDemo("test01").start();
    }
}
