package smartthings.ratpack.kafka

import org.apache.kafka.common.errors.TimeoutException
import ratpack.registry.Registry
import ratpack.service.StartEvent
import ratpack.service.StopEvent
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.lang.Unroll

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

	def "should throw IllegalStateException when message sent while disabled"() {
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

	def "Can send a message with optional properties"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
		config.setLingersMs(0)
		config.setBatchSize(16384)
		config.setSendBufferBytes(131072)
		config.setMaxInFlightRequestsPerConnection(5)
		config.setBufferMemory(33554432)
		config.setAcks("1")

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

	def "check for correct assignment(optional and mandatory fields) in kafka properties"() {
		String clientId="test_clientId"
		Set<String> servers=[getTestKafkaServers()] as Set<String>
		Long maxBlockMillis=1000L
		Long lingersMs=0;
		int batchSize=1234;
		int sendBufferBytes=134567;
		int maxInFlightRequestsPerConnection=5;
		Long bufferMemory=3352;
		String acks="0";

		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId(clientId)
		config.setServers(servers)
		config.setMaxBlockMillis(maxBlockMillis)
		config.setLingersMs(lingersMs)
		config.setBatchSize(batchSize)
		config.setSendBufferBytes(sendBufferBytes)
		config.setMaxInFlightRequestsPerConnection(maxInFlightRequestsPerConnection)
		config.setBufferMemory(bufferMemory)
		config.setAcks(acks)

		when:
		def props = config.getProperties()

		then:
		props.get("kafkaProperties").iterator().size() == 11
		props.get("servers")==servers
		props.get("clientId")==clientId
		props.get("maxBlockMillis")==maxBlockMillis
		props.get("lingersMs")==lingersMs
		props.get("batchSize")==batchSize
		props.get("sendBufferBytes")==sendBufferBytes
		props.get("maxInFlightRequestsPerConnection")==maxInFlightRequestsPerConnection
		props.get("bufferMemory")==bufferMemory
		props.get("acks")==acks
	}

	def "check for correct assignment(only mandatory fields) in kafka properties"() {
		String clientId="test_clientId"
		Set<String> servers=[getTestKafkaServers()] as Set<String>
		Long maxBlockMillis=1000L

		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId(clientId)
		config.setServers(servers)
		config.setMaxBlockMillis(maxBlockMillis)

		when:
		def props = config.getProperties()

		then:
		props.get("kafkaProperties").iterator().size() == 5
		props.get("servers")==servers
		props.get("clientId")==clientId
		props.get("maxBlockMillis")==maxBlockMillis
		props.get("lingersMs")==null
		props.get("batchSize")==null
		props.get("sendBufferBytes")==null
		props.get("maxInFlightRequestsPerConnection")==null
		props.get("bufferMemory")==null
		props.get("acks")==null
	}

	def "Can set and retrieve configs"() {
		Long lingersMs=0;
		int batchSize=16384;
		int sendBufferBytes=131072;
		int maxInFlightRequestsPerConnection=5;
		Long bufferMemory=33554432;
		String acks="1";

		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
		config.setLingersMs(lingersMs)
		config.setBatchSize(batchSize)
		config.setSendBufferBytes(sendBufferBytes)
		config.setMaxInFlightRequestsPerConnection(maxInFlightRequestsPerConnection)
		config.setBufferMemory(bufferMemory)
		config.setAcks(acks)

		KafkaProducerService service

		when:
		service = new KafkaProducerService(config)

		then:
		lingersMs == service.getConfig().getLingersMs()
		batchSize == service.getConfig().getBatchSize()
		sendBufferBytes == service.getConfig().getSendBufferBytes()
		maxInFlightRequestsPerConnection == service.getConfig().getMaxInFlightRequestsPerConnection()
		bufferMemory == service.getConfig().getBufferMemory()
		acks == service.getConfig().getAcks()
	}

	@Unroll
	def "Can send a message with null #property"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
		config.setLingersMs(0)
		config.setBatchSize(16384)
		config.setSendBufferBytes(131072)
		config.setMaxInFlightRequestsPerConnection(5)
		config.setBufferMemory(33554432)
		config.setAcks("1")

		and:
		config[property] = null

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

		where:
		property << ['lingersMs', 'batchSize', 'sendBufferBytes',
					 'maxInFlightRequestsPerConnection', 'bufferMemory', 'acks']
	}

	@Unroll
	def "should throw ConfigException for negative #property value when message sent"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
		config.setAcks("1")

		KafkaProducerService service
		byte[] key = "fake_key".bytes
		byte[] value = "fake_value".bytes

		and:
		config[property] = -1

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
		assert result.throwable.getClass() == org.apache.kafka.common.config.ConfigException
		assert result.throwable.getMessage().contains('Invalid value -1')

		where:
		property << ['lingersMs', 'batchSize', 'sendBufferBytes', 'maxInFlightRequestsPerConnection', 'bufferMemory']
	}

	@Unroll
	def "Can send a message with empty #property"() {
		given:
		KafkaProducerModule.Config config = new KafkaProducerModule.Config()
		config.setClientId("test_clientId")
		config.setServers([getTestKafkaServers()] as Set<String>)
		config.setMaxBlockMillis(1000L)
		config.setLingersMs(0)
		config.setBatchSize(16384)
		config.setSendBufferBytes(131072)
		config.setMaxInFlightRequestsPerConnection(5)
		config.setBufferMemory(33554432)

		KafkaProducerService service
		byte[] key = "fake_key".bytes
		byte[] value = "fake_value".bytes

		and:
		config[property] = ""

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

		where:
		property << ['acks']
	}
}
