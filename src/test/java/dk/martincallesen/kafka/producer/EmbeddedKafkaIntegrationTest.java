package dk.martincallesen.kafka.producer;

import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;

@SpringBootTest
public abstract class EmbeddedKafkaIntegrationTest implements ListenableFutureCallback<SendResult<String, SpecificRecordAdapter>> {

    @Autowired
    protected SpecificRecordProducer producer;

    protected SpecificRecordAdapter actualRecord;
    protected CountDownLatch latch;

    @BeforeEach
    void setupLatch(){
        actualRecord = null;
        latch = new CountDownLatch(1);
    }

    @Override
    public void onSuccess(SendResult<String, SpecificRecordAdapter> sendResult) {
        this.actualRecord = sendResult.getProducerRecord().value();
        latch.countDown();
    }

    @Override
    public void onFailure(Throwable throwable) {}
}
