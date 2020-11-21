package dk.martincallesen.kafka.producer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.Customer;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@EmbeddedKafka(topics = {SpecificRecordProducerIT.ACCOUNT_TOPIC, SpecificRecordProducerIT.CUSTOMER_TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class SpecificRecordProducerIT extends EmbeddedKafkaIntegrationTest {
    public static final String ACCOUNT_TOPIC = "test-account-topic";
    public static final String CUSTOMER_TOPIC = "test-customer-topic";

    @Test
    void isAccountChangeSend() throws InterruptedException {
        Account accountChange = Account.newBuilder()
                .setName("MyAccount")
                .setReg(1234)
                .setNumber(1234567890)
                .build();
        final SpecificRecordAdapter expectedRecord = new SpecificRecordAdapter(accountChange);
        producer.send(ACCOUNT_TOPIC, expectedRecord).addCallback(this);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(expectedRecord, actualRecord, "Sending record");
    }

    @Test
    void isCustomerChangeSend() throws InterruptedException {
        final Customer customerChange = Customer.newBuilder()
                .setFirstName("Michael")
                .setLastName("Hansen")
                .setAge(30)
                .setHeight(180)
                .setWeight(85)
                .setAutomatedEmail(true)
                .build();
        final SpecificRecordAdapter expectedRecord = new SpecificRecordAdapter(customerChange);
        producer.send(CUSTOMER_TOPIC, expectedRecord).addCallback(this);
        latch.await(10, TimeUnit.SECONDS);
        Assertions.assertEquals(expectedRecord, actualRecord);
    }
}
