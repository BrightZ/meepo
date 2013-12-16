package org.meepo.jsender;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonSender {

	public static String get(String urlStr, Map<String, String> param) {

		HttpURLConnection urlConnection = null;

		if (param != null) {
			StringBuffer paramSB = new StringBuffer();
			int i = 0;
			for (String key : param.keySet()) {
				if (i == 0)
					paramSB.append("?");
				else
					paramSB.append("&");

				paramSB.append(key).append("=").append(param.get(key));
				i++;
			}

			urlStr += param;
		}

		try {
			URL url = new URL(urlStr);

			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setDoInput(true);
			urlConnection.setDoOutput(true);
			urlConnection.setRequestProperty("Charset", "utf-8");
			urlConnection
					.setRequestProperty("Content-Type", "application/json");

			return makeContent(urlConnection);

		} catch (Exception e) {
			// Old_TODO
		}

		return null;

	}

	public static String makeContent(HttpURLConnection urlConnection) {
		try {
			InputStream in = urlConnection.getInputStream();
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(in, "UTF-8"));
			StringBuffer temp = new StringBuffer();
			String line = bufferedReader.readLine();
			while (line != null) {
				temp.append(line);
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
			return temp.toString();

		} catch (Exception e) {
			// Old_TODO
		}
		return null;
	}

	public static JSONObject str2JsonObj(String str) {
		if (str == null)
			return null;
		try {
			JSONParser parser = new JSONParser();
			Object obj;
			obj = parser.parse(str);
			JSONObject jObj = (JSONObject) obj;
			return jObj;

		} catch (ParseException e) {
			System.out.println(e);
		}
		return null;
	}

	public static void main(String args[]) {

		// String url = "http://cipher.thu.meepo.org:8888/cipher.json";
		String url = "http://thu.meepo.org/?option=online_number";
		String str = get(url, null);
		System.out.println(str);
		JSONObject obj = str2JsonObj(str);
		System.out.println(obj);
	}

}