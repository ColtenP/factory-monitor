package factory.monitor.config;

import com.typesafe.config.Config;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * The app configuration to power our Flink job
 */
public class AppConfig {
  public final KafkaSourceConfig kafkaSourceConfig;
  public final KafkaSinkConfig kafkaSinkConfig;
  public final SlackConfig slackConfig;

  public AppConfig(KafkaSourceConfig kafkaSourceConfig, KafkaSinkConfig kafkaSinkConfig, SlackConfig slackConfig) {
    this.kafkaSourceConfig = kafkaSourceConfig;
    this.kafkaSinkConfig = kafkaSinkConfig;
    this.slackConfig = slackConfig;
  }

  public static class KafkaSourceConfig {
    public final String bootstrapServers;
    public final Map<String, String> properties;
    public final String topic;

    public KafkaSourceConfig(String bootstrapServers, Map<String, String> properties, String topic) {
      this.bootstrapServers = bootstrapServers;
      this.properties = properties;
      this.topic = topic;
    }

    public static KafkaSourceConfig fromConfig(Config config) {
      return new KafkaSourceConfig(
        config.getString("bootstrap-servers"),
        config.getConfig("properties")
          .entrySet()
          .stream()
          .map(entry -> Map.entry(entry.getKey(), entry.getValue().unwrapped().toString()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        config.getString("topic")
      );
    }
  }

  public static class KafkaSinkConfig {
    public final String bootstrapServers;
    public final Map<String, String> properties;
    public final String avgStdMetricsTopic;
    public final String temperatureReadingsTopic;
    public final String co2EmissionsTopic;
    public final String lowPressureTopic;

    public KafkaSinkConfig(String bootstrapServers, Map<String, String> properties, String avgStdMetricsTopic, String temperatureReadingsTopic, String co2EmissionsTopic, String lowPressureTopic) {
      this.bootstrapServers = bootstrapServers;
      this.properties = properties;
      this.avgStdMetricsTopic = avgStdMetricsTopic;
      this.temperatureReadingsTopic = temperatureReadingsTopic;
      this.co2EmissionsTopic = co2EmissionsTopic;
      this.lowPressureTopic = lowPressureTopic;
    }

    public static KafkaSinkConfig fromConfig(Config config) {
      return new KafkaSinkConfig(
        config.getString("bootstrap-servers"),
        config.getConfig("properties")
          .entrySet()
          .stream()
          .map(entry -> Map.entry(entry.getKey(), entry.getValue().unwrapped().toString()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        config.getString("avg-std-metrics-topic"),
        config.getString("temperature-readings-topic"),
        config.getString("co2-emissions-topic"),
        config.getString("low-pressure-topic")
      );
    }
  }

  public static class SlackConfig {
    public final String url;

    public SlackConfig(String url) {
      this.url = url;
    }

    public static SlackConfig fromConfig(Config config) {
      return new SlackConfig(
        config.getString("url")
      );
    }
  }

  public static AppConfig fromConfig(Config config) {
    return new AppConfig(
      KafkaSourceConfig.fromConfig(config.getConfig("kafka-source-conf")),
      KafkaSinkConfig.fromConfig(config.getConfig("kafka-sink-conf")),
      SlackConfig.fromConfig(config.getConfig("slack-conf"))
    );
  }
}
