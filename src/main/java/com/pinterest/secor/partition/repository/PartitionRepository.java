package com.pinterest.secor.partition.repository;

import com.pinterest.secor.common.SecorConfig;

import java.util.List;

/**
 * Created by rakesh on 11/06/14.
 */
public abstract class PartitionRepository {

	protected SecorConfig mConfig;

	public PartitionRepository(SecorConfig config) {
		mConfig = config;
	}

	abstract void init();

	abstract List<String> fetchPartitionKeys(String topic);


}
