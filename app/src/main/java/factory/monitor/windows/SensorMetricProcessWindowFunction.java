package factory.monitor.windows;

import factory.monitor.model.SensorMetric;
import factory.monitor.model.SensorReading;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SensorMetricProcessWindowFunction extends ProcessWindowFunction<SensorReading, SensorMetric, String, TimeWindow> {
  @Override
  public void process(String entityId,
                      ProcessWindowFunction<SensorReading, SensorMetric, String, TimeWindow>.Context context,
                      Iterable<SensorReading> readings,
                      Collector<SensorMetric> out) {
    List<SensorReading> readingsList = StreamSupport
      .stream(readings.spliterator(), false)
      .collect(Collectors.toList());

    List<Double> values = readingsList
      .stream()
      .map(e -> e.state.changeTo)
      .map(Double::parseDouble)
      .collect(Collectors.toList());

    out.collect(SensorMetric.fromValues(
      entityId,
      context.window().getEnd(),
      readingsList.get(0).state.unit,
      values
    ));
  }
}
