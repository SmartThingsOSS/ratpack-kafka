package smartthings.ratpack.kafka

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

class KafkaConsumerServiceSpec extends Specification {

	String getTestKafkaServers() {
		return System.getenv("KAFKA_SERVER") ?: '127.0.0.1:9092'
	}

	private static final String CLIENT_ID = 'test-client-producer'
	private static final TestData data = new TestData(id: 1, name: 'Hello World')

	TestService testService = Mock(TestService)

	TestConsumer testConsumer = new TestConsumer(testService)

	@AutoCleanup
	@Delegate
	EmbeddedApp app = GroovyEmbeddedApp.of({ spec ->
		registry(Guice.registry { bindings ->
			bindings.moduleConfig(KafkaConsumerModule, new KafkaConsumerModule.Config(), { config ->
				config.setServers([getTestKafkaServers()] as Set<String>)
			})
			bindings.moduleConfig(KafkaProducerModule, new KafkaProducerModule.Config(), { config ->
				config.setServers([getTestKafkaServers()] as Set<String>)
				config.setClientId(CLIENT_ID)
			})
			bindings.bindInstance(TestService, testService)
			bindings.bindInstance(TestConsumer, testConsumer)
		})
		handlers {
			post('produce') { ctx ->
				KafkaProducerService producer = ctx.get(KafkaProducerService)
				ByteArrayOutputStream baos
				ObjectOutputStream oos
				try {
					baos = new ByteArrayOutputStream()
					oos = new ObjectOutputStream(baos)
					oos.writeObject(data)
					byte[] bytes = baos.toByteArray()
					producer.send('test', null, null, bytes, bytes)
						.then({
						ctx.render('ok' + it.toString())
					})
				} catch (IOException ie) {
					ctx.error(ie)
				} catch (Throwable t) {
					ctx.error(t)
				} finally {
					if (oos) {
						oos.close()
					}

					if (baos) {
						baos.close()
					}
				}
			}
		}
	})

	void 'it should consumer Kafka messages'() {
		given:
		def done = new BlockingVariable(2)

		and:
		testService.run(_ as TestData) >> { data ->
			return done.set(data.first())
		}

		when:
		String status = httpClient.postText('produce')

		then:
		println("The call to the producer finished - [status: ${status}]")
		assert (status =~ "^ok")
		def result = done.get()
		assert result.id == data.id
		assert result.name == data.name
	}
}
