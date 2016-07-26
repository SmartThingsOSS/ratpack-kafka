package smartthings.ratpack.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import ratpack.func.Action;

import java.util.Properties;

/**
 * Interface for creating a Kafka Consumer.  Implementers will have their consume method called as messages become
 * available.
 * @param <K> Key deserialization class.
 * @param <V> Value deserialization class.
 */
public interface Consumer<K, V> {
	/**
	 * Method will be called on successful Kafka poll with fetched messages.
	 * @param records
	 */
	void consume(ConsumerRecords<K, V> records) throws Exception;

	/**
	 * Kafka topics in which to subscribe.
	 * @return
	 */
	String[] getTopics();

	/**
	 * Kafka Consumer Group ID.
	 * @return
	 */
	String getGroup();

	/**
	 * Determines how many consumers will be spun up on application start.
	 * @return
     */
	default Integer getConcurrencyLevel() {
		return 1;
	}

	/**
	 * The frequency at which the Kafka consumer client internally polls.
	 * See Apache\'s KafkaConsumer::poll(long timeout)
	 * @return
     */
	default Long getPollWaitTime() {
		return Long.MAX_VALUE;
	}

	/**
	 * Kafka Consumer property overrides.
	 * @return
	 */
	default Properties getKafkaProperties() {
		return new Properties();
	}
}
