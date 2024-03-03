/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package factory.monitor;

import com.typesafe.config.ConfigFactory;
import factory.monitor.config.AppConfig;
import factory.monitor.map.JsonMapFunction;
import factory.monitor.model.SensorReading;
import factory.monitor.process.AvgStdSensorProcessFunction;
import factory.monitor.process.LowPressureProcessFunction;
import factory.monitor.process.TrackHeaterStateProcessFunction;
import factory.monitor.serde.SensorReadingDeserializationSchema;
import factory.monitor.windows.SensorMetricProcessWindowFunction;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Properties;

public class FactoryMonitorJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactoryMonitorJob.class);

  /**
   * Entrypoint into our application which builds the job graph then executes it
   *
   * @param args CLI args
   */
  public static void main(String[] args) throws Exception {
    final AppConfig appConfig = loadAppConfig(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KafkaSource<SensorReading> kafkaSource = constructKafkaSource(appConfig.kafkaSourceConfig);

    DataStreamSource<SensorReading> sensorReadings = env.fromSource(
      kafkaSource,
      WatermarkStrategy
        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(((SerializableTimestampAssigner<SensorReading>) (element, recordTimestamp) -> element.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli())),
      "sensor-readings"
    );

    // Divide our stream into our data types used to make our calculations
    DataStream<SensorReading> heaterReadings = sensorReadings
      .filter((FilterFunction<SensorReading>) reading -> reading.entityId.equalsIgnoreCase("binary_sensor.heater"))
      .name("heater-readings")
      .uid("heater-readings");

    DataStream<SensorReading> temperatureReadings = sensorReadings
      .filter((FilterFunction<SensorReading>) reading -> reading.state.deviceClass.equalsIgnoreCase("temperature"))
      .name("temperature-readings")
      .uid("temperature-readings");

    DataStream<SensorReading> boilerTemperatureReadings = temperatureReadings
      .filter((FilterFunction<SensorReading>) reading -> reading.entityId.contains("boiler_temperature"))
      .name("boiler-temperature-readings")
      .uid("broiler-temperature-readings");

    DataStream<SensorReading> pressureReadings = sensorReadings
      .filter((FilterFunction<SensorReading>) reading -> reading.state.deviceClass.equalsIgnoreCase("pressure"))
      .name("pressure-readings")
      .uid("pressure-readings");

    DataStream<SensorReading> flowReadings = sensorReadings
      .filter((FilterFunction<SensorReading>) reading -> reading.state.deviceClass.equalsIgnoreCase("volume_flow_rate"))
      .name("flow-readings")
      .uid("flow-readings");

    DataStream<SensorReading> co2Readings = sensorReadings
      .filter((FilterFunction<SensorReading>) reading -> reading.state.deviceClass.equalsIgnoreCase("carbon_dioxide"))
      .name("co2-readings")
      .uid("co2-readings");

    // Union together temperature, pressure, and flow readings, create windows of x elements,
    // then emit the avg & std values for each sensor entity
    temperatureReadings
      .union(pressureReadings)
      .union(flowReadings)
      .keyBy((KeySelector<SensorReading, String>) reading -> reading.entityId)
      .process(new AvgStdSensorProcessFunction(15))
      .name("avg-std-metrics")
      .uid("avg-std-metrics")
      .map(new JsonMapFunction<>())
      .name("avg-std-metrics-json")
      .uid("avg-std-metrics-json")
      .sinkTo(constructKafkaSink(appConfig.kafkaSinkConfig, appConfig.kafkaSinkConfig.avgStdMetricsTopic));

    // Union together heater and temperature readings, when the heater turns on send the current temperature for
    // all the temperature sensors.
    heaterReadings
      .union(temperatureReadings)
      .keyBy(new NullByteKeySelector<>())
      .process(new TrackHeaterStateProcessFunction())
      .name("temperature-readings-when-boiler-turns-on")
      .uid("temperature-readings-when-boiler-turns-on")
      .map(new JsonMapFunction<>())
      .name("temperature-readings-when-boiler-turns-on-json")
      .uid("temperature-readings-when-boiler-turns-on-json")
      .sinkTo(constructKafkaSink(appConfig.kafkaSinkConfig, appConfig.kafkaSinkConfig.temperatureReadingsTopic));

    // Union together heater and broiler temperature readings, when the heater turns on open session and calculate

    // Get the sliding average of CO2 emissions within a 5-minute period
    co2Readings
      .keyBy((KeySelector<SensorReading, String>) reading -> reading.entityId)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
      .allowedLateness(Time.minutes(1))
      .process(new SensorMetricProcessWindowFunction())
      .name("co2-sensor-metrics")
      .name("co2-sensor-metrics")
      .map(new JsonMapFunction<>())
      .name("co2-sensor-metrics-json")
      .name("co2-sensor-metrics-json")
      .sinkTo(constructKafkaSink(appConfig.kafkaSinkConfig, appConfig.kafkaSinkConfig.co2EmissionsTopic));

    // Check all the pressure readings, if they dip below 10 for 3 minutes, then trigger an alert
    pressureReadings
      .keyBy((KeySelector<SensorReading, String>) reading -> reading.entityId)
      .process(new LowPressureProcessFunction(10.0, Time.minutes(10).toMilliseconds()))
      .name("low-pressure-alerts")
      .name("low-pressure-alerts")
      .map(new JsonMapFunction<>())
      .name("low-pressure-alerts-json")
      .name("low-pressure-alerts-json")
      .sinkTo(constructKafkaSink(appConfig.kafkaSinkConfig, appConfig.kafkaSinkConfig.lowPressureTopic));

    env.execute("Factory Monitor Job");
  }

  /**
   * Parses the CLI args and tries to get the config path, if none is specified use the internally
   * packaged config
   *
   * @param args The args passed to main
   * @return A path pointing toward the config
   */
  public static AppConfig loadAppConfig(String[] args) {
    Options options = new Options();
    options.addOption("f", "config", true, "The default configuration to use configure the app");

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);

      if (!cmd.hasOption("config")) {
        LOGGER.info("No config was specified in the CLI args");

        InputStreamReader reader = new InputStreamReader(FactoryMonitorJob.class.getResourceAsStream("/conf/application.conf"));
        return AppConfig.fromConfig(ConfigFactory.parseReader(reader));
      }

      return AppConfig.fromConfig(ConfigFactory.parseFile(new File(new URI(cmd.getOptionValue("config")))));
    } catch (ParseException | URISyntaxException | RuntimeException e) {
      LOGGER.error("Could not parse command line arguments");
      LOGGER.error(e.getMessage());
      System.exit(1);
      throw new RuntimeException(e);
    }
  }

  public static KafkaSource<SensorReading> constructKafkaSource(AppConfig.KafkaSourceConfig kafkaSourceConfig) {
    Properties properties = new Properties();
    properties.putAll(kafkaSourceConfig.properties);

    return KafkaSource.<SensorReading>builder()
      .setBootstrapServers(kafkaSourceConfig.bootstrapServers)
      .setTopics(kafkaSourceConfig.topic)
      .setProperties(properties)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SensorReadingDeserializationSchema()))
      .build();
  }

  public static KafkaSink<String> constructKafkaSink(AppConfig.KafkaSinkConfig kafkaSinkConfig, String topic) {
    KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
      .setBootstrapServers(kafkaSinkConfig.bootstrapServers)
      .setTransactionalIdPrefix(topic)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setTransactionalIdPrefix(topic);

    kafkaSinkConfig.properties.forEach(builder::setProperty);

    return builder.build();
  }
}
