package dk.martincallesen.kafka.producer;

import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.slf4j.Logger;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class SendResultLogger {
    private final String topic;
    private final Logger logger;

    public SendResultLogger(String topic, Logger logger) {
        this.topic = topic;
        this.logger = logger;
    }

    public ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>> log(SpecificRecordAdapter event) {
        return new ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.warn("Failed to send {} on {} ", event, topic);
            }

            @Override
            public void onSuccess(SendResult<String, SpecificRecordAdapter> sendResult) {
                logger.info("Successfully send send {} on {} ", event, topic);
            }
        };
    }
}
