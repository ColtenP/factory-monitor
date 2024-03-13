package factory.monitor.process;


import com.fasterxml.jackson.databind.ObjectMapper;
import factory.monitor.model.SlackWebhookBody;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.util.ExceptionUtils;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.http.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class SendSlackMessageAsyncFunction implements AsyncFunction<String, Boolean> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SendSlackMessageAsyncFunction.class);
  public final String slackURL;

  public SendSlackMessageAsyncFunction(String slackURL) {
    this.slackURL = slackURL;
  }

  @Override
  public void asyncInvoke(String input, ResultFuture<Boolean> resultFuture) {
    try {
      Request
        .post(slackURL)
        .bodyString(
          new ObjectMapper().writeValueAsString(new SlackWebhookBody(input)),
          ContentType.APPLICATION_JSON
        )
        .execute()
        .discardContent();
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.stringifyException(e));
    }
    resultFuture.complete(Collections.singleton(true));
  }

  @Override
  public void timeout(String input, ResultFuture<Boolean> resultFuture) {
    resultFuture.complete(Collections.singleton(false));
  }
}
