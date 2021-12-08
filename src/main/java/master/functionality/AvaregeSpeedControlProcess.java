package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

public class AvaregeSpeedControlProcess extends ProcessWindowFunction<TelmeteryDataPoint,
        Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
        Tuple3<Integer, Integer, Integer>, TimeWindow> {


    @Override
    public void process(Tuple3<Integer, Integer, Integer> integerIntegerIntegerTuple3,
                        Context context, Iterable<TelmeteryDataPoint> iterable,
                        Collector<Tuple6<Integer, Integer,
                                Integer, Integer, Integer, Double>> collector) throws Exception {
        Boolean[] allSegments = new Boolean[5]; //52 to 56 inclusive
        Arrays.fill(allSegments, Boolean.FALSE);
        Iterator<TelmeteryDataPoint> iterator = iterable.iterator();

        Integer startTime = Integer.MAX_VALUE;
        Integer finishTime = -1;

        Long startPosition = Long.MAX_VALUE;
        Long finishPosition = -1L;

        TelmeteryDataPoint dp = null;

        while (iterator.hasNext()) {
            dp = iterator.next();

            allSegments[dp.segment - 52] = true;
            startTime = Math.min(startTime, dp.timestamp);
            finishTime = Math.max(finishTime, dp.timestamp);

            //If a car sends several reports on segments 52 or 56,
            // the ones taken for the average speed are the ones that cover a longer distance.
            startPosition = Math.min(startPosition, dp.position);
            finishPosition = Math.max(finishPosition, dp.position);
        }

        //All segments are covered
        if (!Arrays.asList(allSegments).contains(false)) {

            //calculate the average speed in mph
            double avgSpeed = ((finishPosition-startPosition)*1.0 / (finishTime-startTime))*2.23694;

            //Time1, Time2, VID, XWay, Dir, AvgSpd
            collector.collect(new Tuple6<>(startTime, finishTime, dp.vehicleId,
                    dp.highway, dp.direction, avgSpeed));

        }
    }
}
