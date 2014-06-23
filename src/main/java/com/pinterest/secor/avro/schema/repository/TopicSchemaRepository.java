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
		repository.put("avro_test_2__1", schema);

		Schema schema1 = SchemaBuilder.record("V3Logs")
				.namespace("com.webengage.avro.schema")
				.fields()
				.name(SchemaRepositoryUtil.KAFKA_OFFSET).type().optional().longType()
				.name("dt").type().optional().stringType()
				.name("date").type().optional().stringType()
				.name("month").type().optional().stringType()
				.name("year").type().optional().stringType()
				.name("hour").type().optional().stringType()
				.name("min").type().optional().stringType()
				.name("sec").type().optional().stringType()
				.name("host").type().optional().stringType()
				.name("license").type().optional().stringType()
				.name("luid").type().optional().stringType()
				.name("suid").type().optional().stringType()
				.name("status").type().optional().stringType()
				.name("referer").type().optional().stringType()
				.name("agent").type().optional().stringType()
				.endRecord();

		repository.put("v3logs_1", schema1);

	}

	@Override
	public Schema fetchSchema(String topic) {
		return repository.get(topic);
	}

}
