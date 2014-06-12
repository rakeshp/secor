package com.pinterest.secor.uploader.path;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;

/**
 * Created by rakesh on 11/06/14.
 */
public abstract class PathGenerator {

	protected SecorConfig mConfig;

	public PathGenerator(SecorConfig config) {
		mConfig = config;
	}

	public abstract void init();
	public abstract String generateUploadPath(LogFilePath logFilePath);
}
