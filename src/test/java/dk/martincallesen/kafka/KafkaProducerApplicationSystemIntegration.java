package dk.martincallesen.kafka;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import dk.martincallesen.kafka.producer.SpecificRecordProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KafkaProducerApplicationSystemIntegration implements ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>> {

    @Autowired
    private SpecificRecordProducer producer;

    private SpecificRecordAdapter actualRecord;
    private CountDownLatch latch;

    @BeforeEach
    void setupLatch(){
        actualRecord = null;
        latch = new CountDownLatch(1);
    }

    @Test
    void isAccountSendToTopic() throws InterruptedException {
        Account accountChange = Account.newBuilder()
                .setName("CommonAccount")
                .setReg(4321)
                .setNumber(1987654321)
                .build();
        final SpecificRecordAdapter expectedRecord = new SpecificRecordAdapter(accountChange);
        producer.send("account", expectedRecord).addCallback(this);
        latch.await(10, TimeUnit.SECONDS);
        Assertions.assertEquals(expectedRecord, actualRecord);
    }

    @Override
    public void onFailure(Throwable throwable) {}

    @Override
    public void onSuccess(SendResult<String, SpecificRecordAdapter> sendResult) {
        this.actualRecord = sendResult.getProducerRecord().value();
        this.latch.countDown();
    }


}
