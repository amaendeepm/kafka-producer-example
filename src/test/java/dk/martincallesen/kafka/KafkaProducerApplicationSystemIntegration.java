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

    private Account actualAccount;
    private CountDownLatch latch;

    @BeforeEach
    void setupLatch(){
        actualAccount = null;
        latch = new CountDownLatch(1);
    }

    @Test
    void isAccountSendToTopic() throws InterruptedException {
        Account expectedAccount = Account.newBuilder()
                .setName("CommonAccount")
                .setReg(4321)
                .setNumber(1987654321)
                .build();
        producer.send("account", new SpecificRecordAdapter(expectedAccount)).addCallback(this);
        latch.await(10, TimeUnit.SECONDS);
        Assertions.assertEquals(expectedAccount, actualAccount);
    }

    @Override
    public void onFailure(Throwable throwable) {}

    @Override
    public void onSuccess(SendResult<String, SpecificRecordAdapter> stringSpecificRecordAdapterSendResult) {
        this.actualAccount = (Account) stringSpecificRecordAdapterSendResult.getProducerRecord().value().getRecord();
        this.latch.countDown();
    }
}
