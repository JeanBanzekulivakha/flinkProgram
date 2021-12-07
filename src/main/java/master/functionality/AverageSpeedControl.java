package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.streaming.api.datastream.DataStream;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * detects cars with an average speed higher than 60 mph between segments 52 and 56 (both included)
 * in both directions. If a car sends several reports on segments 52 or 56,
 * the ones taken for the average speed are the ones that cover a longer distance..
 */
public class AverageSpeedControl extends Funcionality {

    public AverageSpeedControl(DataStream<TelmeteryDataPoint> events, String folderName) {
        super(events, folderName);
    }

    @Override
    public void run() {
        //can be a custom solution or with the windows (advanced solution)
        events
                .flatMap(new AverageSpeedControlMapping())
                .writeAsCsv(folderName + "avgspeedfines.csv", OVERWRITE)
                .setParallelism(1);
    }

    //avgspeedfines

}
