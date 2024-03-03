package factory.monitor.process;

import factory.monitor.model.PressureAlert;
import factory.monitor.model.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class LowPressureProcessFunction extends KeyedProcessFunction<String, SensorReading, PressureAlert> {
  private transient ValueState<SensorReading> lastReadingState;
  private transient ValueState<Long> timerState;
  private final Double alertThreshold;
  private final Long alertPeriod;

  public LowPressureProcessFunction(Double alertThreshold, Long alertPeriod) {
    this.alertThreshold = alertThreshold;
    this.alertPeriod = alertPeriod;
  }

  @Override
  public void open(Configuration parameters) {
    this.lastReadingState = getRuntimeContext()
      .getState(new ValueStateDescriptor<>("last-reading-state", SensorReading.class));

    this.timerState = getRuntimeContext()
      .getState(new ValueStateDescriptor<>("timer-state", Long.class));
  }

  @Override
  public void processElement(SensorReading reading,
                             KeyedProcessFunction<String, SensorReading, PressureAlert>.Context ctx,
                             Collector<PressureAlert> out) throws IOException {
    lastReadingState.update(reading);
    double value = Double.parseDouble(reading.state.changeTo);

    // If the value is below the threshold, set a timer to trigger an alert
    if (value < alertThreshold) {
      ctx.timerService().registerEventTimeTimer(ctx.timestamp() + alertPeriod);
    } else if (timerState.value() != null) {
      // If the value is above the threshold, and we have a timer set, we need to clear the timer since
      // the reading is now a good state. We should not alert on it
      ctx.timerService().deleteEventTimeTimer(timerState.value());
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<PressureAlert> out) throws IOException {
    out.collect(new PressureAlert(
      ctx.getCurrentKey(),
      timestamp,
      Double.parseDouble(lastReadingState.value().state.changeTo)
    ));
  }
}
