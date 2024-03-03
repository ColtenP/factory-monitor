package factory.monitor.map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class JsonMapFunction<T> extends RichMapFunction<T, String> {
  private transient ObjectMapper mapper;

  @Override
  public void open(Configuration parameters) {
    mapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());
  }

  @Override
  public String map(T value) throws Exception {
    return mapper.writeValueAsString(value);
  }
}
