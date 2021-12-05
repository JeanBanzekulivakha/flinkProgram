package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Detects stopped vehicles on any segment. A vehicle is stopped when it reports at
 * least 4 consecutive events from the same position.
 */
public class AccidentReporter extends Funcionality{

    public AccidentReporter(DataStream<TelmeteryDataPoint> events, String folderName) {
        super(events, folderName);
    }

    @Override
    public void run() {
        events
                .flatMap(new RichFlatMapFunction<TelmeteryDataPoint,
                        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {

                    ValueState<Integer> countValueState;
                    ValueState<Integer> initialTimeValueState;
                    ValueState<Long> lastPositionValueState;

                    @Override
                    public void flatMap(TelmeteryDataPoint telmeteryDataPoint,
                                        Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {

                        //Get the count of same location
                        Integer count = countValueState.value();

                        //Set initial value to zero
                        if (count == null) count = 0;

                        //Check if we have been receiving the last position again
                        if (lastPositionValueState.value() != null &&
                                telmeteryDataPoint.position == lastPositionValueState.value()) {

                            if (initialTimeValueState.value() == null) {
                                initialTimeValueState.update(telmeteryDataPoint.timestamp);
                            }

                            count += 1;


                            countValueState.update(count);
                        } else if (count > 0) {
                            //When last position is different we will reset the count
                            // but only when we the count is already greater than zero to avoid state mgmt
                            count = 0;
                            countValueState.clear();
                            initialTimeValueState.clear();
                        }

                        //remember the last position
                        lastPositionValueState.update(telmeteryDataPoint.position);

                        //Evict the vehicle data if it has reported the same location 4th time onward
                        if (count + 1 >= 4) {
                            //format: Time1, Time2, VID, XWay, Dir, AvgSpd,
                            // where Time1 is the time of the first event
                            //of the segment and Time2 is the time of the last event of the segment.
                            collector.collect(new Tuple6<>(
                                    initialTimeValueState.value(),
                                    telmeteryDataPoint.timestamp,
                                    telmeteryDataPoint.vehicleId,
                                    telmeteryDataPoint.highway,
                                    telmeteryDataPoint.direction,
                                    telmeteryDataPoint.speed //should be zero, right? since car is parked
                            ));
                        }
                    }

                    //Open is called only once when we start the application
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        countValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                        initialTimeValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("initialTimeValueState", BasicTypeInfo.INT_TYPE_INFO));

                        lastPositionValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("lastPositionValueState", BasicTypeInfo.LONG_TYPE_INFO));


                    }
                })
                .writeAsCsv(folderName + "accidents.csv", OVERWRITE);
    }

}
