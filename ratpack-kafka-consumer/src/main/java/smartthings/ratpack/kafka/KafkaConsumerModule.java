package smartthings.ratpack.kafka;

import com.google.inject.Scopes;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import ratpack.guice.ConfigurableModule;

import java.util.Properties;
import java.util.Set;

public class KafkaConsumerModule extends ConfigurableModule<KafkaConsumerModule.Config> {

	protected void configure() {
		bind(KafkaConsumerService.class).in(Scopes.SINGLETON);
	}

	public static class Config {

		Set<String> servers;

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
	}

}
