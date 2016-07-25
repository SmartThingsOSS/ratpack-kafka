package smartthings.ratpack.kafka

import groovy.util.logging.Slf4j
import ratpack.groovy.test.embed.GroovyEmbeddedApp
import ratpack.guice.Guice
import ratpack.test.embed.EmbeddedApp
import smartthings.ratpack.kafka.fixtures.TestConsumer
import smartthings.ratpack.kafka.fixtures.TestData
import smartthings.ratpack.kafka.fixtures.TestProducerService
import smartthings.ratpack.kafka.fixtures.TestService
import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable

import java.time.Instant

@Slf4j
class KafkaConsumerServiceSpec extends Specification {

	String getTestKafkaServers() {
		return System.getenv("KAFKA_SERVER") ?: '127.0.0.1:9092'
	}

	private static final String CLIENT_ID = 'test-client'
	private static final String GROUP_ID = 'test-client'
	private static final String TOPIC = 'test'

	private static final TestData data = new TestData(id: UUID.randomUUID().toString(), name: 'Hello World', timestamp: Instant.now().toEpochMilli())

	TestService testService = Mock(TestService)

	TestConsumer testConsumer = new TestConsumer(testService, GROUP_ID, TOPIC)

	@AutoCleanup
	@Delegate
	EmbeddedApp app = GroovyEmbeddedApp.of({ spec ->
		registry(Guice.registry { bindings ->
			bindings.moduleConfig(KafkaConsumerModule, new KafkaConsumerModule.Config(), { config ->
				config.setServers([getTestKafkaServers()] as Set<String>)
			})
			bindings.bindInstance(TestService, testService)
			bindings.bindInstance(TestConsumer, testConsumer)
			bindings.bindInstance(
				TestProducerService,
				new TestProducerService([getTestKafkaServers()] as Set<String>, CLIENT_ID, TOPIC)
			)
		})
		handlers {
			post('produce') { ctx ->
				TestProducerService producer = ctx.get(TestProducerService)
				try {
					producer.send(data)
						.then({
						ctx.render('ok')
					})
				} catch (Throwable t) {
					ctx.error(t)
				}
			}
		}
	})

	void 'it should consumer Kafka messages'() {
		given:
		def done = new BlockingVariable(90)

		and:
		testService.run(_ as TestData) >> { data ->
			return done.set(data.first())
		}

		when:
		String status = httpClient.postText('produce')

		then:
		log.debug("The call to the producer finished - [status: ${status}]")
		assert status == 'ok'
		def result = done.get()
		assert result.id == data.id
		assert result.name == data.name
		assert result.timestamp == data.timestamp
	}
}
