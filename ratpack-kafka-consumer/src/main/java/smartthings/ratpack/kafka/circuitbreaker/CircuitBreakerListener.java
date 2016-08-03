package smartthings.ratpack.kafka.circuitbreaker;

/**
 * Listener interface providing methods to be called when circuit changes state.
 */
public interface CircuitBreakerListener {

	void opened();

	void closed();

}
