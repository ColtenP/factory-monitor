package factory.monitor.model;

import java.util.List;

public class SensorMetric {
  public final String entityId;
  public final Long timestamp;
  public final Double avg;
  public final Double std;
  public final Double min;
  public final Double max;
  public final String unit;

  public SensorMetric(String entityId, Long timestamp, Double avg, Double std, Double min, Double max, String unit) {
    this.entityId = entityId;
    this.timestamp = timestamp;
    this.avg = avg;
    this.std = std;
    this.min = min;
    this.max = max;
    this.unit = unit;
  }

  public SensorMetric() {
    this("", System.currentTimeMillis(), 0.0, 0.0, 0.0, 0.0, "");
  }

  public static SensorMetric fromValues(String entityId, Long timestamp, String unit, List<Double> values) {
    Double min = values.stream().reduce(Double.MAX_VALUE, Double::min);
    Double max = values.stream().reduce(Double.MIN_VALUE, Double::max);
    Double sum = values.stream().reduce(0.0, Double::sum);
    Double mean = sum / values.size();

    double stdDeviation = 0.0;
    for (Double value : values) {
      stdDeviation += Math.pow(value - mean, 2);
    }
    stdDeviation = Math.sqrt(stdDeviation / values.size());

    return new SensorMetric(entityId, timestamp, mean, stdDeviation, min, max, unit);
  }
}
