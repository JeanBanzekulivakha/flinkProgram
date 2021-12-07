package master.functionality;

import master.TelmeteryDataPoint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentReporterProcess extends ProcessWindowFunction<
        TelmeteryDataPoint,
        Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Long>,
        Tuple2<Integer, Integer>, GlobalWindow> {

    @Override
    public void process(Tuple2<Integer, Integer> integerIntegerTuple2,
                        Context context,
                        Iterable<TelmeteryDataPoint> iterable,
                        Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Long>> collector) throws Exception {

        Iterator<TelmeteryDataPoint> iterator = iterable.iterator();
        TelmeteryDataPoint dp = iterator.next();
        Integer startTime = dp.timestamp;
        long initialPosition = dp.position;
        boolean accident = true;

        int count = 1;

        while (iterator.hasNext()) {
            dp = iterator.next();
            //check if all the 4 has the same position
            if (initialPosition != dp.position) accident = false;
            count += 1;
        }

        if (accident && count >= 4) {
            //Time1,Time2,VID,XWay,Seg,Dir,Pos
            //Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>
            collector.collect(new Tuple7<>(
                    startTime,
                    dp.timestamp,
                    dp.vehicleId,
                    dp.highway,
                    dp.segment,
                    dp.direction,
                    dp.position));
        }
    }

}
