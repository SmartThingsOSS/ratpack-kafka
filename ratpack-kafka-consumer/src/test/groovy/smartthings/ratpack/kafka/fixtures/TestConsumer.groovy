package smartthings.ratpack.kafka.fixtures

import com.google.inject.Inject
import org.apache.kafka.clients.consumer.ConsumerRecords
import smartthings.ratpack.kafka.Consumer

class TestConsumer implements Consumer<byte[], byte[]> {

	String group
	final String[] topics = ['test']

	TestService testService

	@Inject
	TestConsumer(TestService testService) {
		this.testService = testService
		group = 'test-consuemr' + UUID.randomUUID().toString()
	}

	@Override
	void consume(ConsumerRecords<byte[], byte[]> records) throws Exception {
		println("Consuming records. [size: ${records.size()}]")
		records.each { record ->
			ByteArrayInputStream bais
			ObjectInput oi
			try {
				bais = new ByteArrayInputStream(record.value());
				oi = new ObjectInputStream(bais);
				TestData data = (TestData)oi.readObject()
				testService.run(data)
			} catch (IOException e) {
				//ignore
			} finally {
				if (oi) {
					oi.close()
				}
				if (bais) {
					bais.close()
				}
			}

		}
	}
}
