package smartthings.ratpack.kafka;

import com.google.inject.Scopes;
import ratpack.guice.ConfigurableModule;

import java.util.Optional;
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
		boolean enabled = true;
		Optional<Long> lingers_ms;
		Optional<Integer> batch_size;
		Optional<Integer> send_buffer_bytes;
		Optional<Integer> max_in_flight_requests_per_connection;
		Optional<Long> buffer_memory;
		Optional<String> acks;

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

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public void setLingers_ms(Optional<Long> lingers_ms) {
			this.lingers_ms = lingers_ms;
		}

		public void setBatch_size(Optional<Integer> batch_size) {
			this.batch_size = batch_size;
		}

		public void setSend_buffer_bytes(Optional<Integer> send_buffer_bytes) {
			this.send_buffer_bytes = send_buffer_bytes;
		}

		public void setMax_in_flight_requests_per_connection(Optional<Integer> max_in_flight_requests_per_connection) {
			this.max_in_flight_requests_per_connection = max_in_flight_requests_per_connection;
		}

		public void setBuffer_memory(Optional<Long> buffer_memory) {
			this.buffer_memory = buffer_memory;
		}

		public void setAcks(Optional<String> acks) {
			this.acks = acks;
		}

		public Optional<Long> getLingers_ms() {
			return lingers_ms;
		}

		public Optional<Integer> getBatch_size() {
			return batch_size;
		}

		public Optional<Integer> getSend_buffer_bytes() {
			return send_buffer_bytes;
		}

		public Optional<Integer> getMax_in_flight_requests_per_connection() {
			return max_in_flight_requests_per_connection;
		}

		public Optional<Long> getBuffer_memory() {
			return buffer_memory;
		}

		public Optional<String> getAcks() {
			return acks;
		}
	}
}
