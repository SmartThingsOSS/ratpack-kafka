package smartthings.ratpack.kafka.event;

/**
 * Defines all possible events emitted by this Kafka consumer integration.
 */
public class KafkaConsumerEvent {

	public static final KafkaConsumerEvent STARTED_EVENT = new KafkaConsumerEvent("Started");
	public static final KafkaConsumerEvent STOPPED_EVENT = new KafkaConsumerEvent("Stopped");
	public static final KafkaConsumerEvent SUSPENDED_EVENT = new KafkaConsumerEvent("Suspended");
	public static final KafkaConsumerEvent RESUMED_EVENT = new KafkaConsumerEvent("Resumed");

	private final String name;

	private KafkaConsumerEvent(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != getClass()) {
			return false;
		} else if (o == this || ((KafkaConsumerEvent) o).getName().equals(getName())) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return name != null ? name.hashCode() : 0;
	}
}
