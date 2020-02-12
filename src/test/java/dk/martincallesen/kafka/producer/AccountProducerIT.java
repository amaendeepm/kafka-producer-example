package dk.martincallesen.kafka.producer;

import dk.martincallesen.kafka.domain.Account;
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
    private AccountProducer accountProducer;

    @Test
    void sendAccountChangeEvent() {
        Account accountChange = Account.newBuilder()
                .setName("MyAccount")
                .setReg(1234)
                .setNumber(1234567890)
                .build();
        accountProducer.send(TOPIC, accountChange).addCallback(expectSendSuccess());
    }

    private ListenableFutureCallback<SendResult<String, Account>> expectSendSuccess() {
        return new ListenableFutureCallback<SendResult<String, Account>>() {
            @Override
            public void onFailure(Throwable throwable) {
                fail("Sending");
            }

            @Override
            public void onSuccess(SendResult<String, Account> sendResult) {
                assertNotNull(sendResult);
            }
        };
    }
}