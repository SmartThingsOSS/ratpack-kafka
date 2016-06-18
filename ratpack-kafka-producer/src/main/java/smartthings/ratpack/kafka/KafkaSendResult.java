package smartthings.ratpack.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;

public class KafkaSendResult {

	private Optional<RecordMetadata> recordMetadata;
	private Optional<Exception> exception;

	public KafkaSendResult(RecordMetadata recordMetadata, Exception exception) {
		this.recordMetadata = Optional.ofNullable(recordMetadata);
		this.exception = Optional.ofNullable(exception);
	}

	public boolean isSuccessful() {
		return recordMetadata.isPresent();
	}

	public Optional<RecordMetadata> getRecordMetadata() {
		return recordMetadata;
	}

	public Optional<Exception> getException() {
		return exception;
	}
}
