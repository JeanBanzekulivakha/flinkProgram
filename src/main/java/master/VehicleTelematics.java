/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class VehicleTelematics {

    public static void main(String[] args) throws Exception {
        ArgsManager argsManager = new ArgsManager(args);
        final String folderName = argsManager.getPathToOutputFolder() + "/";
        File file = new File(argsManager.getPathToInputFile());
        String absolutePath = file.getAbsolutePath();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //for debugging only
        env.setParallelism(1);
        // Event time is the default setting
        // https://nightlies.apache.org/flink/flink-docs-release-1.12/api/java/org/apache/flink/streaming/api/TimeCharacteristic.html
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Read the data from the file
        DataStream<String> vehiclesData = env.readTextFile(absolutePath);

        // Map individual telemtery events to objects
        // We key by vehicle and for each direction all the time
        // and then apply all the operation on the keyed context

        DataStream<TelmeteryDataPoint> events = vehiclesData
                .map(new MapFunction<String, TelmeteryDataPoint>() {
                    @Override
                    public TelmeteryDataPoint map(String line) throws Exception {
                        return TelmeteryDataPoint.fromString(line);
                    }
                })
                .keyBy(new KeySelector<TelmeteryDataPoint, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(TelmeteryDataPoint telmeteryDataPoint) throws Exception {
                        return new Tuple2<>(telmeteryDataPoint.vehicleId, telmeteryDataPoint.direction);
                    }
                });

        SpeedRadar(events, folderName);
        AverageSpeedControl(events, folderName);
        AccidentReporter(events, folderName);

        env.execute("Vehicle Telematics execution");
    }


    /**
     * Detects cars that overcome the speed limit of 90 mph.
     */
    static void SpeedRadar(DataStream<TelmeteryDataPoint> events, String folderName) {
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
                .writeAsCsv(folderName + "speed-radar.csv", OVERWRITE);

    }

    /**
     * Detects stopped vehicles on any segment. A vehicle is stopped when it reports at
     * least 4 consecutive events from the same position.
     */
    static void AccidentReporter(DataStream<TelmeteryDataPoint> events, String folderName) {

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
                .writeAsCsv(folderName + "accident-reporter.csv", OVERWRITE);
    }

    /**
     * detects cars with an average speed higher than 60 mph between segments 52 and 56 (both included)
     * in both directions. If a car sends several reports on segments 52 or 56,
     * the ones taken for the average speed are the ones that cover a longer distance..
     */
    static void AverageSpeedControl(DataStream<TelmeteryDataPoint> events, String folderName) {

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
                .writeAsCsv(folderName + "average-speed-control.csv", OVERWRITE);
    }
}
