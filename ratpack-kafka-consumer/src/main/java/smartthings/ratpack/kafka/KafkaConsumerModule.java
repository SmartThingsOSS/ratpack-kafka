package smartthings.ratpack.kafka;

import com.google.inject.Scopes;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import ratpack.guice.ConfigurableModule;
import smartthings.ratpack.kafka.circuitbreaker.CircuitBreaker;
import smartthings.ratpack.kafka.circuitbreaker.SimpleCircuitBreaker;

import java.util.Properties;
import java.util.Set;

/**
 * Guice module bindings for the RatPack Kafka consumer library.
 */
public class KafkaConsumerModule extends ConfigurableModule<KafkaConsumerModule.Config> {

	protected void configure() {
		bind(KafkaConsumerService.class).in(Scopes.SINGLETON);
		bind(CircuitBreaker.class).toProvider(SimpleCircuitBreaker::new);
	}

	/**
	 * Primary configuration object for RatPack Kafka Consumer module.
	 */
	public static class Config {

		Set<String> servers;
		boolean enabled = true;

		public Config() {
		}

		public Properties getKafkaProperties() {
			Properties props = new Properties();

			props.put("bootstrap.servers", String.join(",", servers));
			props.put("key.deserializer", ByteArrayDeserializer.class.getName());
			props.put("value.deserializer", ByteArrayDeserializer.class.getName());

			return props;
		}

		public Set<String> getServers() {
			return servers;
		}

		public void setServers(Set<String> servers) {
			this.servers = servers;
		}

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}
	}

}
