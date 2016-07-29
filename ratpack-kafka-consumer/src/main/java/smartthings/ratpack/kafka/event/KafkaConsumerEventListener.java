package smartthings.ratpack.kafka.event;

/**
 * Functional interface for event notification.
 */
public interface KafkaConsumerEventListener {
	void eventNotification(KafkaConsumerEvent event);
}
