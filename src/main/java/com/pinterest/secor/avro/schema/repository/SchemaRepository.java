package com.pinterest.secor.avro.schema.repository;

import com.pinterest.secor.common.SecorConfig;
import org.apache.avro.Schema;

/**
 * Created by rakesh on 10/06/14.
 */
public abstract class SchemaRepository {
	protected SecorConfig mConfig;

	public SchemaRepository(SecorConfig config) {
		mConfig = config;
	}

	public abstract void init();

	public abstract Schema fetchSchema(String topic);



}
