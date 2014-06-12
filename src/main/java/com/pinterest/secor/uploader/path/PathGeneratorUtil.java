package com.pinterest.secor.uploader.path;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.ReflectionUtil;
import org.apache.commons.lang.StringUtils;

/**
 * Created by rakesh on 11/06/14.
 */
public class PathGeneratorUtil {
	private static PathGenerator pathGenerator;

	public static void init(SecorConfig secorConfig) throws Exception {
		String klassName = secorConfig.getUploadPathGeneratorClass();
		if (StringUtils.isNotEmpty(klassName)) {
			pathGenerator = (PathGenerator) ReflectionUtil
					.initializeKlass(klassName,
							secorConfig);
			if (pathGenerator != null) {
				pathGenerator.init();
			}
		}
	}

	public static String generateUploadPath(LogFilePath logFilePath) {
		return pathGenerator.generateUploadPath(logFilePath);
	}
}
