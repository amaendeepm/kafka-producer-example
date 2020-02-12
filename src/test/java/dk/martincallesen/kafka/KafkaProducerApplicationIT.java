package dk.martincallesen.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaProducerApplicationIT {

    @Autowired
    private NewTopic accountTopic;

    @Test
    void isAccountTopicCreated(){
        Assertions.assertNotNull(accountTopic);
    }
}
