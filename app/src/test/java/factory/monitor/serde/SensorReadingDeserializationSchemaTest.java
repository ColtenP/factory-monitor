package factory.monitor.serde;

import factory.monitor.model.SensorReading;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SensorReadingDeserializationSchemaTest {
  @Test
  void canDeserializeSensorMessage() {
    SensorReadingDeserializationSchema serde = new SensorReadingDeserializationSchema();
    String message = "{\"name\": \"Heater\", \"entity_id\": \"binary_sensor.heater\", \"timestamp\": \"2024-02-29 23:15:27\", \"state\": {\"change_from\": \"on\", \"change_to\": \"on\", \"minimum\": \"\", \"maximum\": \"\", \"unit\": \"\", \"device_class\": \"\"}}";
    SensorReading extracted = serde.deserialize(message.getBytes(Charset.defaultCharset()));
    assertNotNull(extracted);

    message = "{\"name\": \"Heater\", \"entity_id\": \"binary_sensor.heater\", \"timestamp\": \"2024-02-29 23:15:27\", \"state\": {\"change_from\": \"on\", \"change_to\": \"on\", \"minimum\": \"\", \"maximum\": \"\", \"unit\": \"\", \"device_class\": \"\"}}";
    extracted = serde.deserialize(message.getBytes(Charset.defaultCharset()));
    assertNotNull(extracted);
  }
}
