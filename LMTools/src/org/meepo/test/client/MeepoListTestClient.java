package org.meepo.test.client;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.client.util.ClientFactory;
import org.meepo.xmlrpc.RpcInterface;

/**
 * This is a sample client for test, notice that you should probably change the
 * Server URL and port
 * 
 * @author MS
 * 
 */

public class MeepoListTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		String urlString = "http://lm2.meepo.thuhpc.org:8080/Meepo/xmlrpc";
		try {
			config.setServerURL(new URL(urlString));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		XmlRpcClient client = new XmlRpcClient();
		client.setConfig(config);
		ClientFactory factory = new ClientFactory(client);
		RpcInterface m = (RpcInterface) factory.newInstance(RpcInterface.class);
		try {
			String token = m.login("msmummy@qq.com", "654312");
			Object[] params;
			for (int i = 0; i < 100; i++) {
				System.out.println("Start to creath the " + i
						+ "th file. \tTime: " + System.currentTimeMillis());
				params = new Object[] { token, "/private/testFile" + i };
				client.execute("Meepo.mkFile", params);
			}

			m.logout(token);

		} catch (XmlRpcException e) {
			e.printStackTrace();
		}

	}
}
