package org.meepo.dsa;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

public class DSClient {

	public int deleteData(Object fileObjectParams[]) {
		try {
			for (int i = 0; i < fileObjectParams.length; i++) {
				logger.info(fileObjectParams[i].toString());
			}
			String base = (String) fileObjectParams[0];
			String filepath = (String) fileObjectParams[1];
			String sha1 = (String) fileObjectParams[2];
			String urlString = base + filepath;
			String params[] = new String[1];
			String values[] = new String[1];
			params[0] = "Z-Token";
			values[0] = sha1;
			return this.connect(urlString, "DELETE", params, values);
		} catch (Exception e) {
			logger.error("", e);
		}
		// if Exception return 0
		return 0;
	}

	public int connect(String urlString, String method, String params[],
			String values[]) {
		URL url;
		int code = 0;
		try {
			logger.info("request for" + urlString + "with method" + method);
			url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setRequestMethod(method);
			for (int i = 0; i < params.length; i++) {
				conn.setRequestProperty(params[i], values[i]);
			}
			conn.connect();
			code = conn.getResponseCode();
			if (code == HttpURLConnection.HTTP_ACCEPTED) {
				logger.info("request accepted");
			} else {
				logger.error(String.format(
						"request denyed.method : %s.code : %d.", method, code));
			}
			return code;
		} catch (MalformedURLException e) {
			logger.error("", e);
		} catch (IOException e) {
			logger.error("", e);
		}
		return code;
	}

	public static DSClient getInstance() {
		return DSClient.dsClient;
	}

	private static DSClient dsClient = new DSClient();

	private static Logger logger = Logger.getLogger(DSClient.class);
}
