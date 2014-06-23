package com.pinterest.secor.uploader.path;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rakesh on 11/06/14.
 */
public class VersionedTopicPathGenerator extends PathGenerator {
	private static final Pattern TOPIC_PATTERN = Pattern.compile("(.*)_(.*)$");

	public VersionedTopicPathGenerator(SecorConfig config) {
		super(config);
	}

	@Override
	public void init() {}

	@Override
	public String generateUploadPath(LogFilePath localPath) {
		String versionedTopic = localPath.getTopic();
		Matcher matcher = TOPIC_PATTERN.matcher(versionedTopic);
		if (matcher.matches()) {
			String topic = matcher.group(1);
			LogFilePath s3Path = new LogFilePath(localPath.getPrefix(), topic,
					localPath.getPartitions(),
					localPath.getGeneration(),
					localPath.getKafkaPartition(),
					localPath.getOffset());

			return s3Path.getLogFilePath();

		} else {
			return localPath.getLogFilePath();
		}
	}
}
