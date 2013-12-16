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

public class MeepoReplicaTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		// String urlString = "http://lm1.thu.meepo.org:8080/meepo/xmlrpc";
		String urlString = "http://127.0.0.1:8080/meepo/xmlrpc";
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
			String token = m.login(username, password);
			Object[] params = new Object[] { token, "/Groups/" };
			Object[] objs = (Object[]) client.execute("Meepo.listDir", params);
			for (Object obj : objs) {
				System.out.println(obj);
			}
			m.logout(token);

		} catch (XmlRpcException e) {
			e.printStackTrace();
		}

	}

	private static String username = "ck99@mails.tsinghua.edu.cn";
	private static String password = "asdf1234";
}
