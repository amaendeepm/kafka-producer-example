package dk.martincallesen.kafka;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.Customer;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import dk.martincallesen.kafka.producer.EmbeddedKafkaIntegrationTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static dk.martincallesen.kafka.producer.KafkaProducerConfig.ACCOUNT_TOPIC;
import static dk.martincallesen.kafka.producer.KafkaProducerConfig.CUSTOMER_TOPIC;

class KafkaProducerApplicationSystemIntegration extends EmbeddedKafkaIntegrationTest {

    @Test
        void isAccountChangeSend() throws InterruptedException {
        Account accountChange = Account.newBuilder()
                .setName("CommonAccount")
                .setReg(4321)
                .setNumber(1987654321)
                .build();
        final SpecificRecordAdapter expectedRecord = new SpecificRecordAdapter(accountChange);
        SpecificRecordAdapter actualRecord = sendRecordTo(ACCOUNT_TOPIC, expectedRecord);
        Assertions.assertEquals(expectedRecord, actualRecord);
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
        SpecificRecordAdapter actualRecord = sendRecordTo(CUSTOMER_TOPIC, expectedRecord);
        Assertions.assertEquals(expectedRecord, actualRecord);
    }
}
