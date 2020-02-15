package dk.martincallesen.kafka;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import dk.martincallesen.kafka.producer.SpecificRecordProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootTest
class KafkaProducerApplicationSystemIntegration {

    @Autowired
    private SpecificRecordProducer specificRecordProducer;

    @Test
    void isAccountSendToTopic() {
        Account accountChange = Account.newBuilder()
                .setName("CommonAccount")
                .setReg(4321)
                .setNumber(1987654321)
                .build();
        final SpecificRecordAdapter record = new SpecificRecordAdapter(accountChange);
        specificRecordProducer.send(record).addCallback(new ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>>() {
            @Override
            public void onFailure(Throwable throwable) {
                Assertions.fail("Failed to send account");
            }

            @Override
            public void onSuccess(SendResult<String, SpecificRecordAdapter> sendResult) {
                Assertions.assertNotNull(sendResult, "Failed to send account");
            }
        });
    }
}
