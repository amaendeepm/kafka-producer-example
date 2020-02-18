package dk.martincallesen.kafka.producer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = AccountProducerIT.TOPIC,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class AccountProducerIT implements ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>>{
    public static final String TOPIC = "test-account-topic";

    @Autowired
    private SpecificRecordProducer producer;

    private SpecificRecordAdapter actualRecord;
    private CountDownLatch latch;

    @BeforeEach
    void setup(){
        actualRecord = null;
        latch = new CountDownLatch(1);
    }

    @Test
    void sendAccountChangeEvent() throws InterruptedException {
        Account expectedAccount = Account.newBuilder()
                .setName("MyAccount")
                .setReg(1234)
                .setNumber(1234567890)
                .build();
        final SpecificRecordAdapter expectedRecord = new SpecificRecordAdapter(expectedAccount);
        producer.send(TOPIC, expectedRecord).addCallback(this);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(expectedRecord, actualRecord, "Sending record");
    }

    @Override
    public void onFailure(Throwable throwable) {}

    @Override
    public void onSuccess(SendResult<String, SpecificRecordAdapter> sendResult) {
        this.actualRecord = sendResult.getProducerRecord().value();
        latch.countDown();
    }
}
