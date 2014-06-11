package com.pinterest.secor.partition.repository;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.ReflectionUtil;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by rakesh on 10/06/14.
 */
public class PartitionRepositoryUtil {
	private static PartitionRepository partitionRepository;

	public static void init(SecorConfig secorConfig) throws Exception {
		String klassName = secorConfig.getTopicPartitionSchemaRepositoryClass();
		if (StringUtils.isNotEmpty(klassName)) {
			partitionRepository = (PartitionRepository) ReflectionUtil
					.initializeKlass(klassName,
							secorConfig);
			if (partitionRepository != null) {
				partitionRepository.init();
			}
		}
	}

	public static List<String> getPartitionKeys(String topic) {
		return partitionRepository.fetchPartitionKeys(topic);
	}
}
