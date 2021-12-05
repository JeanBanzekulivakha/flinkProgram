package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Detects cars that overcome the speed limit of 90 mph.
 */
public class SpeedRadar extends Funcionality{

    public SpeedRadar(DataStream<TelmeteryDataPoint> events, String folderName) {
        super(events, folderName);
    }

    @Override
    public void run() {
        events
                .filter(new FilterFunction<TelmeteryDataPoint>() {
                    @Override
                    public boolean filter(TelmeteryDataPoint telmeteryDataPoint) throws Exception {
                        return telmeteryDataPoint.speed > 90;
                    }
                })
                .map(new MapFunction<TelmeteryDataPoint, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(
                            TelmeteryDataPoint telmeteryDataPoint) throws Exception {

                        //format: Time, VID, XWay, Seg, Dir, Spd
                        return new Tuple6<>(
                                telmeteryDataPoint.timestamp,
                                telmeteryDataPoint.vehicleId,
                                telmeteryDataPoint.highway,
                                telmeteryDataPoint.segment,
                                telmeteryDataPoint.direction,
                                telmeteryDataPoint.speed);
                    }
                })
                .writeAsCsv(folderName + "speedfines.csv", OVERWRITE);
    }
}
