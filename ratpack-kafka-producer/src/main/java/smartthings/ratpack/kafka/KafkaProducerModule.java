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
		Optional<Long> lingersMs;
		Optional<Integer> batchSize;
		Optional<Integer> sendBufferBytes;
		Optional<Integer> maxInFlightRequestsPerConnection;
		Optional<Long> bufferMemory;
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
			props.put("linger.ms", lingersMs);
			props.put("batch.size", batchSize);
			props.put("send.buffer.bytes", sendBufferBytes);
			props.put("max.in.flight.requests.per.connection", maxInFlightRequestsPerConnection);
			props.put("buffer.memory", bufferMemory);
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

		public Optional<Long> getLingersMs() {
			return lingersMs;
		}

		public Optional<Integer> getBatchSize() {
			return batchSize;
		}

		public Optional<Integer> getSendBufferBytes() {
			return sendBufferBytes;
		}

		public Optional<Integer> getMaxInFlightRequestsPerConnection() {
			return maxInFlightRequestsPerConnection;
		}

		public Optional<Long> getBufferMemory() {
			return bufferMemory;
		}

		public Optional<String> getAcks() {
			return acks;
		}

		public void setLingersMs(Long lingersMs) {
			this.lingersMs = Optional.ofNullable(lingersMs);
		}

		public void setBatchSize(Integer batchSize) {
			this.batchSize = Optional.ofNullable(batchSize);
		}

		public void setSendBufferBytes(Integer sendBufferBytes) {
			this.sendBufferBytes = Optional.ofNullable(sendBufferBytes);
		}

		public void setMaxInFlightRequestsPerConnection(Integer maxInFlightRequestsPerConnection) {
			this.maxInFlightRequestsPerConnection = Optional.ofNullable(maxInFlightRequestsPerConnection);
		}

		public void setBufferMemory(Long bufferMemory) {
			this.bufferMemory = Optional.ofNullable(bufferMemory);
		}

		public void setAcks(String acks) {
			this.acks = Optional.ofNullable(acks);
		}
	}
}
