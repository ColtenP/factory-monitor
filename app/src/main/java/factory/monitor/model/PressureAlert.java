package factory.monitor.model;

public class PressureAlert {
  public final String entityId;
  public final Long timestamp;
  public final Double value;


  public PressureAlert(String entityId, Long timestamp, Double value) {
    this.entityId = entityId;
    this.timestamp = timestamp;
    this.value = value;
  }
}
