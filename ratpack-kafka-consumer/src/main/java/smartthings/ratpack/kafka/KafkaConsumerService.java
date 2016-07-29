package smartthings.ratpack.kafka;

import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import ratpack.exec.Execution;
import ratpack.service.Service;
import ratpack.service.StartEvent;
import ratpack.service.StopEvent;

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

	@Inject
	public KafkaConsumerService(KafkaConsumerModule.Config config) {
		this.config = config;
	}

	@Override
	public void onStart(StartEvent event) {
		// Find all configured Consumer implementations from Registry.
		Iterator<? extends Consumer> it = event.getRegistry().getAll(TypeToken.of(Consumer.class)).iterator();

		// Populate our actions in accordance with the desired concurrency level.
		if (it != null && it.hasNext()){
			it.forEachRemaining(consumer -> {
				int concurrency = consumer.getConcurrencyLevel();

				IntStream
					.rangeClosed(1, concurrency)
					.forEach((action) -> actions.add(new ConsumerAction(consumer, config)));
			});
		}

		// Kick off a new execution for each defined consumer.
		if (!actions.isEmpty()) {
			actions.forEach((action) -> Execution.fork().start(action));
		}
	}


	@Override
	public void onStop(StopEvent event) {
		if (!actions.isEmpty()) {
			actions.forEach(ConsumerAction::shutdown);
		}
	}
}
