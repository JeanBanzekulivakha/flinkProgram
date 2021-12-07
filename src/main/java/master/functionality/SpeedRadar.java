package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Detects cars that overcome the speed limit of 90 mph.
 */
public class SpeedRadar extends Funcionality {

    public static final int MAXIMUM_SPEED_LIMIT = 90;

    public SpeedRadar(DataStream<TelmeteryDataPoint> events, String folderName) {
        super(events, folderName);
    }

    @Override
    public void run() {
        events
                .filter((TelmeteryDataPoint p) -> p.speed > MAXIMUM_SPEED_LIMIT)
                .map(new ToOutput())
                .writeAsCsv(folderName + "speedfines.csv", OVERWRITE)
                .setParallelism(1);
    }

    public static class ToOutput implements MapFunction<TelmeteryDataPoint, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> {

        @Override
        public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(TelmeteryDataPoint point) throws Exception {
            return new Tuple6<>(
                    point.timestamp,
                    point.vehicleId,
                    point.highway,
                    point.segment,
                    point.direction,
                    point.speed);
        }

    }
}
