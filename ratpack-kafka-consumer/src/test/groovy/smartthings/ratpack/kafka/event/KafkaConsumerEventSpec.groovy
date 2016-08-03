package smartthings.ratpack.kafka.event

import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.StandardCharsets

class KafkaConsumerEventSpec extends Specification {

	@Unroll
	void 'it should determine object equality'() {

		when:
		boolean bool = object1.equals(object2)

		then:
		assert bool == result

		where:
		object1                           |  object2                          | result
		KafkaConsumerEvent.RESUMED_EVENT  |  KafkaConsumerEvent.STOPPED_EVENT | false
		KafkaConsumerEvent.RESUMED_EVENT  |  KafkaConsumerEvent.RESUMED_EVENT | true
		KafkaConsumerEvent.RESUMED_EVENT  |  StandardCharsets.UTF_8           | false
		KafkaConsumerEvent.RESUMED_EVENT  |  null                             | false
	}

	@Unroll
	void 'it should retrieve an event name'() {
		when:
		String name = event.getName()

		then:
		name == eventName

		where:
		event                              | eventName
		KafkaConsumerEvent.STARTED_EVENT   | KafkaConsumerEvent.STARTED_EVENT.getName()
		KafkaConsumerEvent.STOPPED_EVENT   | KafkaConsumerEvent.STOPPED_EVENT.getName()
		KafkaConsumerEvent.SUSPENDED_EVENT | KafkaConsumerEvent.SUSPENDED_EVENT.getName()
		KafkaConsumerEvent.RESUMED_EVENT   | KafkaConsumerEvent.RESUMED_EVENT.getName()
	}
}
