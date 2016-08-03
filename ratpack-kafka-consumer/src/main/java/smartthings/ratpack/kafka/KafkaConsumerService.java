package smartthings.ratpack.kafka;

import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import ratpack.exec.Execution;
import ratpack.service.Service;
import ratpack.service.StartEvent;
import ratpack.service.StopEvent;
import smartthings.ratpack.kafka.circuitbreaker.CircuitBreaker;
import smartthings.ratpack.kafka.circuitbreaker.CircuitBreakerListener;
import smartthings.ratpack.kafka.event.KafkaConsumerEvent;
import smartthings.ratpack.kafka.event.KafkaConsumerEventListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Service responsible for managing the lifecycle of all Kafka Consumer implementations defined within Registry.
 */
public class KafkaConsumerService implements Service {

	private KafkaConsumerModule.Config config;
	private List<ConsumerAction> actions = new ArrayList<>();
	private List<KafkaConsumerEventListener> listeners = new ArrayList<>();
	private CircuitBreaker circuitBreaker;

	@Inject
	public KafkaConsumerService(KafkaConsumerModule.Config config) {
		this.config = config;
	}

	@Override
	public void onStart(StartEvent event) {
		// Find all configured Consumer implementations from Registry.
		Iterator<? extends Consumer> it = event.getRegistry().getAll(TypeToken.of(Consumer.class)).iterator();
		circuitBreaker = event.getRegistry().get(CircuitBreaker.class);
		circuitBreaker.init(new CircuitBreakerListener() {
			@Override
			public void opened() {
				notifyEventListeners(KafkaConsumerEvent.SUSPENDED_EVENT);
			}

			@Override
			public void closed() {
				notifyEventListeners(KafkaConsumerEvent.RESUMED_EVENT);
			}
		});

		// Populate our actions in accordance with the desired concurrency level.
		if (it != null && it.hasNext()){
			it.forEachRemaining(consumer -> {
				int concurrency = consumer.getConcurrencyLevel();
				registerEventListener((e) -> consumer.eventHandler(e));

				IntStream
					.rangeClosed(1, concurrency)
					.forEach((action) -> actions.add(new ConsumerAction(consumer, config, circuitBreaker)));
			});
		}

		// Kick off a new execution for each defined consumer.
		if (!actions.isEmpty()) {
			actions.forEach((action) -> Execution.fork().start(action));
		}

		notifyEventListeners(KafkaConsumerEvent.STARTED_EVENT);
	}

	@Override
	public void onStop(StopEvent event) {
		if (!actions.isEmpty()) {
			actions.forEach(ConsumerAction::shutdown);
		}
		notifyEventListeners(KafkaConsumerEvent.STOPPED_EVENT);
	}

	/**
	 * Suspend polling on all KafkaConsumers.
	 */
	public void suspend() {
		if (circuitBreaker == null) {
			throw new IllegalStateException("KafkaConsumerService is missing circuit breaker.");
		}
		circuitBreaker.open();
	}

	/**
	 * Informs on whether all KafkaConsumers are suspended.
	 */
	public boolean isSuspended() {
		if (circuitBreaker == null) {
			throw new IllegalStateException("KafkaConsumerService is missing circuit breaker.");
		}
		return circuitBreaker.isOpen();
	}

	/**
	 * Continue polling and consuming Kafka messages.
	 */
	public void resume() {
		if (circuitBreaker == null) {
			throw new IllegalStateException("KafkaConsumerService is missing circuit breaker.");
		}
		circuitBreaker.close();
	}

	/**
	 * Register an event listener.  Alternatively, override event handler on Consumer.
	 * @param listener
	 */
	public void registerEventListener(KafkaConsumerEventListener listener) {
		listeners.add(listener);
	}

	private void notifyEventListeners(KafkaConsumerEvent event) {
		listeners.forEach((listener) -> listener.eventNotification(event));
	}
}
