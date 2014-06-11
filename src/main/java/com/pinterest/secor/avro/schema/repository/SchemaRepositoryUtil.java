package com.pinterest.secor.avro.schema.repository;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.ReflectionUtil;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;

/**
 * Created by rakesh on 10/06/14.
 */
public class SchemaRepositoryUtil {
	public static final String KAFKA_OFFSET = "kafkaoffset";
	private static SchemaRepository schemaRepository;

	public static void init(SecorConfig secorConfig) throws Exception {
		String klassName = secorConfig.getTopicAvroSchemaRepositoryClass();
		if (StringUtils.isNotEmpty(klassName)) {
			schemaRepository = (SchemaRepository) ReflectionUtil
					.initializeKlass(klassName,
							secorConfig);
			if (schemaRepository != null) {
				schemaRepository.init();
			}
		}
	}

	public static Schema getTopicSchema(String topic) {
		return schemaRepository.fetchSchema(topic);
	}
}
