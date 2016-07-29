package smartthings.ratpack.kafka.fixtures

import groovy.util.logging.Slf4j
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import ratpack.exec.Blocking
import ratpack.exec.Promise
import ratpack.service.Service
import ratpack.service.StartEvent
import ratpack.service.StopEvent

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@Slf4j
class TestProducerService implements Service {

	KafkaProducer<byte[], byte[]> kafkaProducer
	Set<String> servers
	String clientId
	String topic

	TestProducerService(Set<String> servers, String clientId, String topic) {
		this.servers = servers
		this.clientId = clientId
		this.topic = topic
	}

	@Override
	public void onStart(StartEvent event) throws Exception {
		kafkaProducer = new KafkaProducer<>(getKafkaProperties())
		kafkaProducer.partitionsFor("test")
	}

	@Override
	public void onStop(StopEvent event) throws Exception {
		kafkaProducer.close()
	}

	public Promise<RecordMetadata> send(TestData data) {
		byte[] key = data.id.getBytes(StandardCharsets.UTF_8)
		byte[] value
		ByteArrayOutputStream baos
		ObjectOutputStream oos
		try {
			baos = new ByteArrayOutputStream()
			oos = new ObjectOutputStream(baos)
			oos.writeObject(data)
			value = baos.toByteArray()
		} finally {
			if (oos) {
				oos.close()
			}

			if (baos) {
				baos.close()
			}
		}

		return Blocking.get({
			log.debug("Sending a message. [topic: ${topic}, data: ${data}]")
			kafkaProducer.send(new ProducerRecord<>(topic, key, value)).get()
		});
	}

	Properties getKafkaProperties() {
		Properties props = new Properties()
		props.put('bootstrap.servers', String.join(',', servers))
		props.put('client.id', clientId)
		props.put('key.serializer', 'org.apache.kafka.common.serialization.ByteArraySerializer')
		props.put('value.serializer', 'org.apache.kafka.common.serialization.ByteArraySerializer')
		props.put('max.block.ms', TimeUnit.MINUTES.toMillis(1))
		return props
	}

}
