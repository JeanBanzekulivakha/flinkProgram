package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

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
                .flatMap(new RichFlatMapFunction<TelmeteryDataPoint,
                        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {

                    //t1,last,count,sum
                    ValueState<Tuple4<Integer, Integer, Integer, Integer>> valuesState;
                    ArrayList<Integer> segmentsToCheck = new ArrayList<>(Arrays.asList(52, 53, 54, 55, 56));

                    @Override
                    public void flatMap(TelmeteryDataPoint telmeteryDataPoint,
                                        Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {

                        Tuple4<Integer, Integer, Integer, Integer> metaData = valuesState.value();
                        //one of the possible segments
                        if (segmentsToCheck.contains(telmeteryDataPoint.segment)) {


                            if (metaData == null || metaData.f0 == -1) {
                                //if this is the first time because we don't have the start time yet
                                valuesState.update(new Tuple4<>(telmeteryDataPoint.timestamp,
                                        telmeteryDataPoint.segment,
                                        1,
                                        telmeteryDataPoint.speed));
                            } else {
                                //for every other entry, we will keep adding
                                valuesState.update(new Tuple4<>(metaData.f0,
                                        telmeteryDataPoint.segment,
                                        metaData.f2 + 1,
                                        metaData.f3 + telmeteryDataPoint.speed));
                            }
                        } else {

                            if (metaData != null && metaData.f0 != -1 && metaData.f2 >= 5) {
                                //if there is a start state, then we will check and emit the speed
                                // when there were 5 or more segments, calculate the average
                                Integer avg = metaData.f3 / metaData.f2;
                                if (avg > 60) {
                                    //Time1, Time2, VID, XWay, Dir, AvgSpd
                                    collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(metaData.f0,
                                            telmeteryDataPoint.timestamp,
                                            telmeteryDataPoint.vehicleId,
                                            telmeteryDataPoint.highway,
                                            telmeteryDataPoint.direction,
                                            avg));
                                }
                            }

                            //we will reset all the values if there is a different segment than what we are looking for
                            valuesState.update(new Tuple4<>(-1, telmeteryDataPoint.segment, -1, -1));

                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {

//                        countValueState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));
//
//                        initialTimeValueState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<Integer>("initialTimeValueState", BasicTypeInfo.INT_TYPE_INFO));
//
//
//                        sumValueState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<Integer>("sumValueState", BasicTypeInfo.INT_TYPE_INFO));
//
//                        lastSegmentValueState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<Integer>("lastSegmentValueState", BasicTypeInfo.INT_TYPE_INFO));

                        valuesState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple4<Integer, Integer, Integer, Integer>>("lastSegmentValueState",
                                        TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>() {
                                        })));


                    }
                })
                .writeAsCsv(folderName + "avgspeedfines.csv", OVERWRITE);
    }

}
