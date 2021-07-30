package dz.spark;

import org.pcap4j.packet.Packet;
import java.util.LinkedList;
import java.util.List;
/**
 * Приложение является реаилзацией тестового задания к собеседованию
 * Приложение предназначено для подсчета количетсва пакетов прохоядщих
 * через выбранное устройство при помощи Spark-streaming, kafka, pcap4j
 *
 * Разработано и протестировано на ОС Windows 10
 *
 * Для работы необходимы:
 * Java Runtime Environment
 * PostgreSQL
 * Npcap - https://nmap.org/npcap/
 * Zookeeper
 * Apache Kafka
 */
public class PacketCounter {
    public static final long SLEEP_TIME = 5000;

    static List<Packet> packets = new LinkedList<>();
    static long numberOfPackets;
    static long min = 0;
    static long max = 0;

    public static void main(String[] args){
        Thread pcapController = new Thread(new PacketController());
        pcapController.start();
    }
}

