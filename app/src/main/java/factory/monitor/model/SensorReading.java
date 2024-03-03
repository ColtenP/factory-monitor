package factory.monitor.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.LocalDateTime;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class SensorReading {
  public String name;
  public String entityId;
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
  public LocalDateTime timestamp;

  public SensorState state;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static class SensorState {
    public String changeFrom;
    public String changeTo;
    public String minimum;
    public String maximum;
    public String unit;
    public String deviceClass;
  }
}
