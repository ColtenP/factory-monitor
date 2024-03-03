package factory.monitor.process;

import factory.monitor.model.SensorReading;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TrackHeaterStateProcessFunction extends KeyedProcessFunction<Byte, SensorReading, SensorReading> {

  private transient MapState<String, SensorReading> temperatureReadingMapState;

  @Override
  public void open(Configuration parameters) {
    this.temperatureReadingMapState = getRuntimeContext()
      .getMapState(new MapStateDescriptor<>("temperature-readings", String.class, SensorReading.class));
  }


  @Override
  public void processElement(SensorReading reading, KeyedProcessFunction<Byte, SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
    if (reading.entityId.contains("heater")) {
      if (!reading.state.changeTo.equalsIgnoreCase("on")) return;

      for (SensorReading temperatureReading : temperatureReadingMapState.values()) {
        out.collect(temperatureReading);
      }
    } else {
      temperatureReadingMapState.put(reading.entityId, reading);
    }
  }
}
