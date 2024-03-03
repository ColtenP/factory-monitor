package factory.monitor.process;

import factory.monitor.model.SensorMetric;
import factory.monitor.model.SensorReading;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AvgStdSensorProcessFunction extends KeyedProcessFunction<String, SensorReading, SensorMetric> {

  private transient ListState<SensorReading> sensorReadingListState;
  private final Integer readingCount;

  public AvgStdSensorProcessFunction(Integer readingCount) {
    this.readingCount = readingCount;
  }

  @Override
  public void open(Configuration parameters) {
    this.sensorReadingListState = getRuntimeContext()
      .getListState(new ListStateDescriptor<>("sensor-readings", SensorReading.class));
  }

  @Override
  public void processElement(SensorReading reading,
                             KeyedProcessFunction<String, SensorReading, SensorMetric>.Context ctx,
                             Collector<SensorMetric> out) throws Exception {
    List<SensorReading> currentReadings = StreamSupport.stream(sensorReadingListState.get().spliterator(), false)
      .collect(Collectors.toList());

    if (currentReadings.size() >= this.readingCount) {
      List<Double> readingValues = currentReadings.stream()
        .map(event -> Double.parseDouble(event.state.changeTo))
        .collect(Collectors.toList());

      out.collect(SensorMetric.fromValues(
        ctx.getCurrentKey(),
        ctx.timerService().currentWatermark(),
        currentReadings.get(0).state.unit,
        readingValues
      ));

      sensorReadingListState.clear();
      sensorReadingListState.add(reading);
    } else {
      sensorReadingListState.add(reading);
    }
  }
}
