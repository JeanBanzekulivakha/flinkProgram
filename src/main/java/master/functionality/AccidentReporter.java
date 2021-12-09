package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Detects stopped vehicles on any segment. A vehicle is stopped when it reports at
 * least 4 consecutive events from the same position.
 */
public class AccidentReporter extends Funcionality{

    public static final int NO_SPEED = 0;

    public AccidentReporter(DataStream<TelmeteryDataPoint> events, String folderName) {
        super(events, folderName);
    }

    @Override
    public void run() {
        events
                .filter((TelmeteryDataPoint p) -> p.speed == NO_SPEED)
                .keyBy(new PersonalKeySelector())
                .countWindow(4, 1)
                .process(new AccidentReporterProcess())
                .writeAsCsv(folderName + "accidents.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }

    public static class PersonalKeySelector implements KeySelector<TelmeteryDataPoint, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> getKey(TelmeteryDataPoint point) throws Exception {
            return new Tuple2<>(point.vehicleId, point.direction);
        }

    }

}
