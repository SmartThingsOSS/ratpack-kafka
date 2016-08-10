package smartthings.ratpack.kafka

import com.google.common.reflect.TypeToken
import ratpack.registry.Registry
import ratpack.service.StartEvent
import smartthings.ratpack.kafka.circuitbreaker.CircuitBreaker
import smartthings.ratpack.kafka.circuitbreaker.SimpleCircuitBreaker
import spock.lang.Specification

class KafkaConsumerServiceSpec extends Specification {

	void 'it should not startup when config marked as disabled'() {
		given:
		KafkaConsumerModule.Config config = new KafkaConsumerModule.Config()
		config.setEnabled(false);
		KafkaConsumerService service = new KafkaConsumerService(config)
		StartEvent event = Mock(StartEvent)

		when:
		service.onStart(event)

		then:
		0 * _
	}

	void 'it should call startup when config marked as enabled'() {
		given:
		KafkaConsumerModule.Config config = new KafkaConsumerModule.Config()
		config.setEnabled(true);
		KafkaConsumerService service = new KafkaConsumerService(config)
		StartEvent event = Mock()
		Registry registry = Mock()
		CircuitBreaker breaker = new SimpleCircuitBreaker();

		when:
		service.onStart(event)

		then:
		_ * event.getRegistry() >> registry
		1 * registry.getAll(TypeToken.of(Consumer.class)) >> []
		1 * registry.get(CircuitBreaker.class) >> breaker
		0 * _
	}
}
