package com.pinterest.secor.partition.repository;

import com.pinterest.secor.common.SecorConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rakesh on 10/06/14.
 */
public class TopicPartitionRepository extends PartitionRepository {

	Map<String, List<String>> repository;

	public TopicPartitionRepository(SecorConfig config) {
		super(config);
	}

	@Override
	public void init() {
		repository = new HashMap<String, List<String>>();
		repository.put("avro_test_2", Arrays.asList("license", "date"));
		repository.put("avro_test_2__1", Arrays.asList("license", "date"));
		repository.put("v3logs_1", Arrays.asList("license", "date"));
	}

	@Override
	List<String> fetchPartitionKeys(String topic) {
		return repository.get(topic);
	}

}
