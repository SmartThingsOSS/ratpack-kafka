package smartthings.ratpack.kafka.fixtures

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import ratpack.exec.Blocking
import ratpack.exec.Promise
import ratpack.service.Service
import ratpack.service.StartEvent
import ratpack.service.StopEvent

import java.util.concurrent.TimeUnit

class TestProducerService implements Service {

	KafkaProducer<byte[], byte[]> kafkaProducer
	Set<String> servers
	String clientId

	TestProducerService(Set<String> servers, String clientId) {
		this.servers = servers
		this.clientId = clientId
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

	public Promise<RecordMetadata> send(String topic, byte[] key, byte[] value) {
		return Blocking.get({
			println("Sending a message. [topic: ${topic}]")
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
