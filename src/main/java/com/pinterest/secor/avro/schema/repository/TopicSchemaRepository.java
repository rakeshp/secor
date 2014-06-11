package com.pinterest.secor.avro.schema.repository;

import com.pinterest.secor.common.SecorConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rakesh on 10/06/14.
 */
public class TopicSchemaRepository extends SchemaRepository {

	Map<String, Schema> repository;

	public TopicSchemaRepository(SecorConfig config) {
		super(config);
	}

	@Override
	public void init() {
		repository = new HashMap<String, Schema>();

		Schema schema = SchemaBuilder.record("TestData")
				.namespace("com.webengage.avro_test")
				.fields()
				.name(SchemaRepositoryUtil.KAFKA_OFFSET).type().optional().longType()
				.name("license").type().optional().stringType()
				.name("date").type().optional().stringType()
				.name("message").type().optional().stringType()
				.endRecord();
		repository.put("avro_test_2", schema);
	}

	@Override
	public Schema fetchSchema(String topic) {
		return repository.get(topic);
	}

}
