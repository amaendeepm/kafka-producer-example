package dk.martincallesen.kafka.producer;

import dk.martincallesen.kafka.domain.Account;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class AccountProducer {
    private KafkaTemplate<String, Account> kafkaTemplate;

    public AccountProducer(KafkaTemplate<String, Account> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ListenableFuture<SendResult<String, Account>> send(String topic, Account accountEvent) {
        return kafkaTemplate.send(topic, accountEvent);
    }
}
