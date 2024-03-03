package factory.monitor.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import factory.monitor.model.SensorReading;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class SensorReadingDeserializationSchema implements DeserializationSchema<SensorReading> {

  private final Logger LOGGER = LoggerFactory.getLogger(SensorReadingDeserializationSchema.class);
  private transient ObjectMapper mapper;

  public void open(InitializationContext context) {
    mapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());
  }

  @Override
  public SensorReading deserialize(byte[] message) {
    String messageJson = new String(message, StandardCharsets.UTF_8);

    try {
      return mapper.readValue(messageJson, SensorReading.class);
    } catch (Exception e) {
      LOGGER.error("Could not deserialize incoming sensor reading: {}", messageJson);
      LOGGER.error(e.toString());
      return null;
    }
  }

  @Override
  public boolean isEndOfStream(SensorReading nextElement) {
    return false;
  }

  @Override
  public TypeInformation<SensorReading> getProducedType() {
    return TypeInformation.of(SensorReading.class);
  }
}
