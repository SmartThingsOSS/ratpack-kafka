package smartthings.ratpack.kafka

import org.apache.kafka.common.errors.TimeoutException
import ratpack.registry.Registry
import ratpack.service.StartEvent
import ratpack.service.StopEvent
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification

class KafkaProducerServiceSpec extends Specification {

	String getTestKafkaServers() {
		return System.getenv("KAFKA_SERVER") ?: '127.0.0.1:9092'
	}

	@AutoCleanup
	ExecHarness harness = ExecHarness.harness()

	def "Fail on bad config"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setMaxBlockMillis(500L)
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
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
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
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
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

	def "should throw IllegalStateExeception when message sent while disabled"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
		config.setEnabled(false);
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
		assert result.error
		assert result.throwable.getClass() == IllegalStateException
		assert result.throwable.getMessage() == 'KafkaProducer is currently not available.'
	}
}
