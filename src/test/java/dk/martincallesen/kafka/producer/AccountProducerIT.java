package dk.martincallesen.kafka.producer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.concurrent.ListenableFutureCallback;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = AccountProducerIT.TOPIC,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class AccountProducerIT {
    public static final String TOPIC = "test-account-topic";

    @Autowired
    private SpecificRecordProducer specificRecordProducer;

    @Test
    void sendAccountChangeEvent() {
        Account accountEvent = Account.newBuilder()
                .setName("MyAccount")
                .setReg(1234)
                .setNumber(1234567890)
                .build();
        final SpecificRecordAdapter recordAdapter = new SpecificRecordAdapter(accountEvent);
        specificRecordProducer.send(TOPIC, recordAdapter).addCallback(expectSendSuccess());
    }

    private ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>> expectSendSuccess() {
        return new ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>>() {
            @Override
            public void onFailure(Throwable throwable) {
                fail("Sending");
            }

            @Override
            public void onSuccess(SendResult<String, SpecificRecordAdapter> sendResult) {
                assertNotNull(sendResult, "Sending");
            }
        };
    }
}
