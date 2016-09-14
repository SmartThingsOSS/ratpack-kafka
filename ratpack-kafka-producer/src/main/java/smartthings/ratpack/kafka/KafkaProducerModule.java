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
		boolean enabled = true;
		long lingers_ms = 0;
		int batch_size = 16384;
		int send_buffer_bytes = 131072;
		int max_in_flight_requests_per_connection = 5;
		long buffer_memory = 3354432;
		String acks = "1";

		public Config() {
		}

		public Properties getKafkaProperties() {
			Properties props = new Properties();

			props.put("bootstrap.servers", String.join(",", servers));
			props.put("client.id", clientId);
			props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			props.put("max.block.ms", maxBlockMillis);
			props.put("linger.ms", lingers_ms);
			props.put("batch.size", batch_size);
			props.put("send.buffer.bytes", send_buffer_bytes);
			props.put("max.in.flight.requests.per.connection", max_in_flight_requests_per_connection);
			props.put("buffer.memory", buffer_memory);
			props.put("acks", acks);

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

		public long getLingers_ms() {
			return lingers_ms;
		}

		public int getBatch_size() {
			return batch_size;
		}

		public int getSend_buffer_bytes() {
			return send_buffer_bytes;
		}

		public int getMax_in_flight_requests_per_connection() {
			return max_in_flight_requests_per_connection;
		}

		public long getBuffer_memory() {
			return buffer_memory;
		}

		public String getAcks() {
			return acks;
		}

		public void setLingers_ms(long lingers_ms) {
			this.lingers_ms = lingers_ms;
		}

		public void setBatch_size(int batch_size) {
			this.batch_size = batch_size;
		}

		public void setSend_buffer_bytes(int send_buffer_bytes) {
			this.send_buffer_bytes = send_buffer_bytes;
		}

		public void setMax_in_flight_requests_per_connection(int max_in_flight_requests_per_connection) {
			this.max_in_flight_requests_per_connection = max_in_flight_requests_per_connection;
		}

		public void setBuffer_memory(long buffer_memory) {
			this.buffer_memory = buffer_memory;
		}

		public void setAcks(String acks) {
			this.acks = acks;
		}
	}
}
