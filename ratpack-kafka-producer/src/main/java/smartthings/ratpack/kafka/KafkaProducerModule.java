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

		private Set<String> servers;
		private String clientId;
		private Long maxBlockMillis = TimeUnit.MINUTES.toMillis(1);
		private boolean enabled = true;
		private Integer lingersMs;
		private Integer batchSize;
		private Integer sendBufferBytes;
		private Integer maxInFlightRequestsPerConnection;
		private Long bufferMemory;
		private String acks;

		public Config() {
		}

		public Properties getKafkaProperties() {

			Properties props = new Properties();

			props.put("bootstrap.servers", String.join(",", servers));
			props.put("client.id", clientId);
			props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			props.put("max.block.ms", maxBlockMillis);
			if (lingersMs != null) {
				props.put("linger.ms", lingersMs);
			}
			if (batchSize != null) {
				props.put("batch.size", batchSize);
			}
			if (sendBufferBytes != null) {
				props.put("send.buffer.bytes", sendBufferBytes);
			}
			if (maxInFlightRequestsPerConnection != null) {
				props.put("max.in.flight.requests.per.connection", maxInFlightRequestsPerConnection);
			}
			if (bufferMemory != null) {
				props.put("buffer.memory", bufferMemory);
			}
			if (acks != null && !acks.isEmpty()) {
				props.put("acks", acks);
			}

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

		public Integer getLingersMs() {
			return lingersMs;
		}

		public void setLingersMs(Integer lingersMs) {
			this.lingersMs = lingersMs;
		}

		public Integer getBatchSize() {
			return batchSize;
		}

		public void setBatchSize(Integer batchSize) {
			this.batchSize = batchSize;
		}

		public Integer getSendBufferBytes() {
			return sendBufferBytes;
		}

		public void setSendBufferBytes(Integer sendBufferBytes) {
			this.sendBufferBytes = sendBufferBytes;
		}

		public Integer getMaxInFlightRequestsPerConnection() {
			return maxInFlightRequestsPerConnection;
		}

		public void setMaxInFlightRequestsPerConnection(Integer maxInFlightRequestsPerConnection) {
			this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
		}

		public Long getBufferMemory() {
			return bufferMemory;
		}

		public void setBufferMemory(Long bufferMemory) {
			this.bufferMemory = bufferMemory;
		}

		public String getAcks() {
			return acks;
		}

		public void setAcks(String acks) {
			this.acks = acks;
		}
	}
}
