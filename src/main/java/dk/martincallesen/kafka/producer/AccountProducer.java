package dk.martincallesen.kafka.producer;

import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;

@Component
public class AccountProducer {
    public static final String TOPIC = "account";
    private SendResultLogger logger = new SendResultLogger(TOPIC, LoggerFactory.getLogger(AccountProducer.class));
    private KafkaTemplate<String, SpecificRecordAdapter> kafkaTemplate;

    public AccountProducer(KafkaTemplate<String, SpecificRecordAdapter> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ListenableFuture<SendResult<String, SpecificRecordAdapter>> send(SpecificRecordAdapter accountEvent) {
        return send(TOPIC, accountEvent);
    }

    public ListenableFuture<SendResult<String, SpecificRecordAdapter>> send(String topic, SpecificRecordAdapter accountEvent) {
        String key = UUID.randomUUID().toString();
        final ListenableFuture<SendResult<String, SpecificRecordAdapter>> sendResult = kafkaTemplate.send(topic, key, accountEvent);
        sendResult.addCallback(logger.log(accountEvent));

        return sendResult;
    }
}
