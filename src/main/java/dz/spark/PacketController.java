package dz.spark;

import org.pcap4j.core.*;
import org.pcap4j.util.NifSelector;

import java.io.IOException;

public class PacketController implements Runnable {
    static PcapNetworkInterface getNetWorkInterface() {
        PcapNetworkInterface device = null;
        try {
            device = new NifSelector().selectNetworkInterface();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return device;
    }

    @Override
    public void run() {
        PcapNetworkInterface device = getNetWorkInterface();
        System.out.println("Device: " + device);

        if (device == null) {
            System.out.println("No device");
            System.exit(1);
        }

        //Запускаем счетчик пакетов, срабатывает каждые SLEEP_TIME милисекунд
        Thread sparkController = new Thread(new SparkController());
        sparkController.start();

        //Обновление значений min и max из БД, срабатывает каждые 20 минут
        Thread limitsController = new Thread(new LimitsController());
        limitsController.start();

        //Захват пакетов на выбранном контроллере
        int snapshotLength = 65536;
        int readTimeout = 50;
        final PcapHandle handle;

        try {
            PacketListener listener = packet -> PacketCounter.packets.add(packet);
            handle = device.openLive(snapshotLength, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, readTimeout);
            handle.loop(-1, listener);
            handle.close();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (NotOpenException e) {
            e.printStackTrace();
        } catch (PcapNativeException e) {
            e.printStackTrace();
        }
    }
}
