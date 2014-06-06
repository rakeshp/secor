package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rakesh on 03/06/14.
 */
public class WeLogParser extends MessageParser {

	private static final String LOG_REGEX = "[^ ]* [^\\s]* \\[([0-9]*)/([a-zA-Z]*)/([0-9]*):([0-9]*):([0-9]*):([0-9]*) [^ ]* ([^ ]*) - [0-9a-zA-Z]+ REST.GET.OBJECT [^ ]* \"GET \\/webengage-[z]?files\\/webengage\\/([~]?[0-9a-z]+)\\/v3.js\\?r=[0-9]+\\&u=([0-9a-zA-Z]*)\\|([0-9]*) HTTP/[^ ]* ([0-9]*) [\\-0-9a-zA-Z\\s]* \"([^\"]*)\" \"([^\"]*)\" \\-";
	private static final Pattern LOG_PATTERN = Pattern.compile(LOG_REGEX);

	public WeLogParser(SecorConfig config) {
		super(config);
	}

	@Override
	public String[] extractPartitions(Message payload) throws Exception {


		byte[] payload1 = payload.getPayload();
		String s = new String(payload1);
		Matcher matcher = LOG_PATTERN.matcher(s);
		if (matcher.matches()) {
			String date = matcher.group(1);
			String month = matcher.group(2);
			String year = matcher.group(3);
			String license = matcher.group(8);
			return new String[]{"license=" + license, "dt=" + year + "-" + month + "-" + date};
		} else {
			return new String[0];
		}
	}
}
