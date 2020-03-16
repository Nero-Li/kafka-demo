package org.lyming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName KafkaProducerDemo
 * @Description TODO
 * @Author lyming
 * @Date 2020/3/3 10:50 下午
 **/
public class KafkaProducerDemo extends Thread {

    private final KafkaProducer<Integer, String> producer;

    private final String topic;

    public KafkaProducerDemo(String topic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.2.201:9092,192.168.2.202:9092,192.168.2.203:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaProducerId");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.topic = topic;
        producer = new KafkaProducer<Integer, String>(properties);
    }

    @Override
    public void run() {
        int num = 0;
        while (num < 10) {
            String message = "message_" + num;
            System.out.println("Begin to send message:" + message);
            producer.send(new ProducerRecord(topic, message));
            num++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        new KafkaProducerDemo("test01").start();
    }
}
