package smartthings.ratpack.kafka.fixtures

import groovy.transform.ToString

@ToString
class TestData implements Serializable {
	static final long serialVersionUID = 1L;

	String id
	String name
	Long timestamp
}
