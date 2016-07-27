package smartthings.ratpack.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Execution;
import ratpack.exec.Promise;
import ratpack.func.Action;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

/**
 * Action definition for continuous polling of Kafka messages.
 */
class ConsumerAction implements Action<Execution> {

	private static final Logger log = LoggerFactory.getLogger(ConsumerAction.class);

	private final Consumer consumer;
	private final KafkaConsumerModule.Config config;
	private final KafkaConsumer client;

	ConsumerAction(Consumer consumer, KafkaConsumerModule.Config config) {
		this.consumer = consumer;
		this.config = config;
		this.client = new KafkaConsumer(getKafkaProperties());
	}

	void shutdown() {
		client.wakeup();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Execution execution) throws Exception {
		client.subscribe(new HashSet<>(Arrays.asList(consumer.getTopics())));

		poll().
			result(r -> {
				if (r.isError()) {
					Throwable error = r.getThrowable();
					if (error instanceof WakeupException) {
						log.warn("KafkaConsumer is shutting down " + toString());
					} else {
						log.error("UnexpectedError KafkaConsumer is shutting down " + toString(), error);
					}
					client.close();
				}
			});
	}

	@SuppressWarnings("unchecked")
	private Promise<?> poll() {
		return Blocking.get(() -> client.poll(consumer.getPollWaitTime()))
			.operation(consumer::consume)
			.flatMap(this::poll);
	}

	private Properties getKafkaProperties() {
		// Apply default values.
		Properties props = config.getKafkaProperties();
		props.put("group.id", consumer.getGroup());

		// Apply overrides
		props.putAll(consumer.getKafkaProperties());

		return props;
	}

	@Override
	public String toString() {
		return "Consumer{" +
				"class=" + consumer.getClass().getSimpleName() +
				", topics=[" + String.join(",", Arrays.asList(consumer.getTopics())) + "]" +
				", group=" + consumer.getGroup() +
				'}';
	}
}
