package dk.martincallesen.kafka.producer;

import dk.martincallesen.kafka.domain.Account;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.UUID;

@Component
public class AccountProducer {
    public static final String TOPIC = "account";
    private Logger logger = LoggerFactory.getLogger(AccountProducer.class);
    private KafkaTemplate<String, Account> kafkaTemplate;
    public AccountProducer(KafkaTemplate<String, Account> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ListenableFuture<SendResult<String, Account>> send(Account accountChange) {
        return send(TOPIC, accountChange);
    }

    public ListenableFuture<SendResult<String, Account>> send(String topic, Account accountEvent) {
        String key = UUID.randomUUID().toString();
        final ListenableFuture<SendResult<String, Account>> sendResult = kafkaTemplate.send(topic, key, accountEvent);
        sendResult.addCallback(logSendResult(accountEvent));

        return sendResult;
    }

    private ListenableFutureCallback<SendResult<String, Account>> logSendResult(Account accountEvent) {
        return new ListenableFutureCallback<SendResult<String, Account>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.warn("Failed to send Account: "+accountEvent);
            }

            @Override
            public void onSuccess(SendResult<String, Account> stringAccountSendResult) {
                logger.info("Successfully send Account: "+accountEvent);
            }
        };
    }
}
