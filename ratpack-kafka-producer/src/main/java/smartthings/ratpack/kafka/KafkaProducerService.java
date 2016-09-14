package smartthings.ratpack.kafka;

import com.google.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.service.Service;
import ratpack.service.StartEvent;
import ratpack.service.StopEvent;

/**
 * Service class to wrap logic of interacting with Kafka to produce messages.
 */
public class KafkaProducerService implements Service {

	private KafkaProducer<byte[], byte[]> kafkaProducer;

	private final KafkaProducerModule.Config config;

	@Inject
	public KafkaProducerService(KafkaProducerModule.Config config) {
		this.config = config;
	}

	@Override
	public void onStart(StartEvent event) throws Exception {
		if (config.isEnabled()) {
			kafkaProducer = new KafkaProducer<>(config.getKafkaProperties());
			kafkaProducer.partitionsFor("test");
		}
	}

	@Override
	public void onStop(StopEvent event) throws Exception {
		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
	}

	public Promise<RecordMetadata> send(String topic, Integer partition, Long timestamp, byte[] key, byte[] value) {

		if (kafkaProducer == null) {
			return Promise.error(new IllegalStateException("KafkaProducer is currently not available."));
		}

		// Due to the fact that send can block in situations we need to treat this whole thing as blocking if https://issues.apache.org/jira/browse/KAFKA-3539 is fixed we can start consuming it as we normally would.
		return Blocking.get(() ->
			kafkaProducer.send(new ProducerRecord<>(topic, partition, timestamp, key, value)).get()
		);
	}

	public KafkaProducerModule.Config getConfig() {
		return config;
	}
}
