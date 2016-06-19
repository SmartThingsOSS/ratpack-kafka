package smartthings.ratpack.kafka

import org.apache.kafka.common.errors.TimeoutException
import ratpack.registry.Registry
import ratpack.service.StartEvent
import ratpack.service.StopEvent
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification

class KafkaProducerServiceSpec extends Specification {

	@AutoCleanup
	ExecHarness harness = ExecHarness.harness()

	def "Fail on bad config"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setMaxBlockMillis(200L)
		config.setServers(['localhost:1'] as Set<String>)
		KafkaProducerService service

		when:
		harness.run {
			service = new KafkaProducerService(config)
			service.onStart(new StartEvent() {
				@Override
				Registry getRegistry() {
					return Registry.empty()
				}

				@Override
				boolean isReload() {
					return false
				}
			})
		}

		then:
		thrown(TimeoutException)
	}

	def "Can send a message"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers(['192.168.99.100:9092'] as Set<String>)
		config.setMaxBlockMillis(500L)
		KafkaProducerService service
		byte[] key = "fake_key".bytes
		byte[] value = "fake_value".bytes

		when:
		def result = harness.yield {
			service = new KafkaProducerService(config)
			service.onStart(new StartEvent() {
				@Override
				Registry getRegistry() {
					return Registry.empty()
				}

				@Override
				boolean isReload() {
					return false
				}
			})

			return service.send("test", null, null, key, value)
		}

		then:
		result.getValueOrThrow()
	}

	def "Can start and stop as expected"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers(['192.168.99.100:9092'] as Set<String>)
		config.setMaxBlockMillis(500L)
		KafkaProducerService service
		byte[] key = "fake_key".bytes
		byte[] value = "fake_value".bytes

		when:
		harness.run {
			service = new KafkaProducerService(config)
			service.onStart(new StartEvent() {
				@Override
				Registry getRegistry() {
					return Registry.empty()
				}

				@Override
				boolean isReload() {
					return false
				}
			})
		}

		and:
		harness.run {
			service.onStop(new StopEvent() {
				@Override
				Registry getRegistry() {
					return Registry.empty()
				}

				@Override
				boolean isReload() {
					return false
				}
			})
		}

		then:
		noExceptionThrown()
	}
}
