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

import master.functionality.AccidentReporter;
import master.functionality.AverageSpeedControl;
import master.functionality.SpeedRadar;
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

        //The program must be optimized to run on a Flink cluster with 3 task manager slots available
        env.setParallelism(3);

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

        new SpeedRadar(events, folderName).run();
        new AverageSpeedControl(events, folderName).run();
        new AccidentReporter(events, folderName).run();

        env.execute("Vehicle Telematics execution");
    }
}
