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

public class MeepoDirReplicaTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		String urlString = "http://lm1.meepo.thuhpc.org:8080/Meepo/xmlrpc";
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
			String token = m.login("wangqiuping816@gmail.com", "huaisha");
			Object[] params;
			// System.out.println("Start to creath the " + i +
			// "th file. \tTime: " + System.currentTimeMillis());
			params = new Object[] { token, "/private/" };
			Object objs[] = (Object[]) client.execute(
					"Meepo.listAllReplicatedDirs", params);
			for (int i = 0; i < objs.length; i++) {
				Object[] oos = (Object[]) objs[i];
				for (int j = 0; j < oos.length; j++) {
					System.out.println(oos[j]);
				}
			}
			// Object objs[][] = (Object[][])
			// client.execute("Meepo.listAllReplicatedDirs", params);
			// for (int i=0; i<objs.length; i++) {
			// System.out.println(objs[i][0] + " " + objs[i][1]);
			// }

			m.logout(token);

		} catch (XmlRpcException e) {
			e.printStackTrace();
		}

	}
}
