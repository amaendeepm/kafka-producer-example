package dk.martincallesen.kafka;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.kafka.producer.AccountProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootTest
class KafkaProducerApplicationSystemIntegration {

    @Autowired
    private AccountProducer accountProducer;

    @Test
    void isAccountSendToTopic() {
        Account accountChange = Account.newBuilder()
                .setName("CommonAccount")
                .setReg(4321)
                .setNumber(1987654321)
                .build();
        accountProducer.send(accountChange).addCallback(new ListenableFutureCallback<SendResult<String, Account>>() {
            @Override
            public void onFailure(Throwable throwable) {
                Assertions.fail("Failed to send account");
            }

            @Override
            public void onSuccess(SendResult<String, Account> stringAccountSendResult) {
                Assertions.assertNotNull(stringAccountSendResult, "Failed to send account");
            }
        });
    }
}
