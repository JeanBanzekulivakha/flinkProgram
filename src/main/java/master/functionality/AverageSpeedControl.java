package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * detects cars with an average speed higher than 60 mph between segments 52 and 56 (both included)
 * in both directions. If a car sends several reports on segments 52 or 56,
 * the ones taken for the average speed are the ones that cover a longer distance..
 */
public class AverageSpeedControl extends Funcionality {

    private static final int MIN_SEGMENT = 52;
    private static final int MAX_SEGMENT = 56;

    public AverageSpeedControl(DataStream<TelmeteryDataPoint> events, String folderName) {
        super(events, folderName);
    }

    @Override
    public void run() {
        //can be a custom solution or with the windows (advanced solution)
        events
                .filter((TelmeteryDataPoint p) -> p.segment >= MIN_SEGMENT && p.segment <= MAX_SEGMENT)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TelmeteryDataPoint>() {
                    @Override
                    public long extractAscendingTimestamp(TelmeteryDataPoint telmeteryDataPoint) {
                        return telmeteryDataPoint.timestamp * 1000;
                    }
                })
                .keyBy(new PersonalKeySelector())
                //we need to have all the segement 52 to 56 and each will be reported once means 180 seconds or more
                .window(EventTimeSessionWindows.withGap(Time.seconds(180)))
                .process(new AvaregeSpeedControlProcess())
                .writeAsCsv(folderName + "avgspeedfines.csv", OVERWRITE)
                .setParallelism(1);
    }

    public static class PersonalKeySelector implements KeySelector<TelmeteryDataPoint, Tuple3<Integer, Integer, Integer>> {

        @Override
        public Tuple3<Integer, Integer, Integer> getKey(TelmeteryDataPoint point) throws Exception {
            return new Tuple3<>(point.vehicleId, point.highway, point.direction);
        }

    }

}
