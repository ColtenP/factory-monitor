package factory.monitor.windows;

import org.apache.flink.api.common.functions.AggregateFunction;

public class SlackMessageAggregateFunction implements AggregateFunction<String, StringBuilder, String> {
  @Override
  public StringBuilder createAccumulator() {
    return new StringBuilder();
  }

  @Override
  public StringBuilder add(String value, StringBuilder accumulator) {
    return accumulator.append(value).append("\n");
  }

  @Override
  public String getResult(StringBuilder accumulator) {
    String result = accumulator.substring(0, 3800);
    return result.substring(0, result.lastIndexOf('\n'));
  }

  @Override
  public StringBuilder merge(StringBuilder a, StringBuilder b) {
    return a.append(b);
  }
}
