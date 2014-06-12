package com.pinterest.secor.uploader.path;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;

/**
 * Created by rakesh on 11/06/14.
 */
public class DefaultPathGenerator extends PathGenerator {
	public DefaultPathGenerator(SecorConfig config) {
		super(config);
	}

	@Override
	public void init() {}

	@Override
	public String generateUploadPath(LogFilePath logFilePath) {
		return logFilePath.getLogFilePath();
	}
}
