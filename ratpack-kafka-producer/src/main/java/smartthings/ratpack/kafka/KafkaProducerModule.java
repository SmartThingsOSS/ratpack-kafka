package smartthings.ratpack.kafka;

import com.google.inject.Scopes;
import ratpack.guice.ConfigurableModule;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Guice module bindings for the RatPack Kafka producer library.
 */
public class KafkaProducerModule extends ConfigurableModule<KafkaProducerModule.Config> {

	@Override
	protected void configure() {
		bind(KafkaProducerService.class).in(Scopes.SINGLETON);
	}

	/**
	 * Primary configuration object for RatPack Kafka Producer module.
	 */
	public static class Config {

		Set<String> servers;
		String clientId;
		Long maxBlockMillis = TimeUnit.MINUTES.toMillis(1);

		public Config() {
		}

		public Properties getKafkaProperties() {
			Properties props = new Properties();

			props.put("bootstrap.servers", String.join(",", servers));
			props.put("client.id", clientId);
			props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			props.put("max.block.ms", maxBlockMillis);

			return props;
		}

		public Set<String> getServers() {
			return servers;
		}

		public void setServers(Set<String> servers) {
			this.servers = servers;
		}

		public String getClientId() {
			return clientId;
		}

		public void setClientId(String clientId) {
			this.clientId = clientId;
		}

		public Long getMaxBlockMillis() {
			return maxBlockMillis;
		}

		public void setMaxBlockMillis(Long maxBlockMillis) {
			this.maxBlockMillis = maxBlockMillis;
		}
	}
}
