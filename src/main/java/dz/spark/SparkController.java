package dz.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.pcap4j.packet.Packet;

import java.util.LinkedList;
import java.util.Properties;

public class SparkController implements Runnable {
    private SparkConf sparkConf;
    private JavaSparkContext javaSparkContext;
    private KafkaProducer<String, String> producer;

    public SparkController() {
        this.sparkConf = new SparkConf().setMaster("local").setAppName("SparkTrafficCounter");
        this.javaSparkContext = new JavaSparkContext(sparkConf);
        this.producer = initKafkaProducer();
    }

    public KafkaProducer<String,String> initKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String,String>(properties);
    }

    @Override
    public void run(){
        while(true) {
            //TODO: Добавить возможность считать пакеты связанные только с одним ip-адресом
            JavaRDD<Packet> packetJavaRDD = javaSparkContext.parallelize(PacketCounter.packets);
            PacketCounter.numberOfPackets = packetJavaRDD.count();
            PacketCounter.packets = new LinkedList<Packet>();

            if (PacketCounter.numberOfPackets > PacketCounter.max || PacketCounter.numberOfPackets < PacketCounter.min){
                producer.send(new ProducerRecord<String, String>("alert", "WARNING! Traffic limit is reached."));
            }
            try {
                Thread.sleep(PacketCounter.SLEEP_TIME);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }
}
