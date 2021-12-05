package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.streaming.api.datastream.DataStream;

abstract class Funcionality {

    protected final DataStream<TelmeteryDataPoint> events;
    protected final String folderName;

    protected Funcionality(DataStream<TelmeteryDataPoint> events, String folderName) {
        this.events = events;
        this.folderName = folderName;
    }

    abstract public void run();
}
