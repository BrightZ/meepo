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

public class MeepoProxyTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		String urlString = "http://127.0.0.1:8080/Meepo/xmlrpc";
		// String urlString = "http://lm1.meepo.thuhpc.org:8080/Meepo/xmlrpc";
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
			String token = m.login("msmummy@gmail.com", "654312");
			long startTime;
			long endTime;
			// Clean garbage if exists.
			// try{
			// m.delete(token, BASE_PATH);
			// } catch (XmlRpcException e) {
			// }
			//
			// m.mkDir(token, BASE_PATH);
			// startTime = System.currentTimeMillis();
			// //Create 1000 file
			// for (int i = 0; i < LOOP; i++) {
			// m.mkFile(token, BASE_PATH + i);
			// }
			// endTime = System.currentTimeMillis();
			// System.out.println("Create " + LOOP +" files in " + (endTime -
			// startTime) + "ms.");
			//
			// //Create 1000 folders
			// startTime = System.currentTimeMillis();
			// String b = BASE_PATH;
			// for (int i = 0; i < FOLDER_LOOP; i++) {
			// b = b + "folder" + i + "/";
			// m.mkDir(token, b);
			// }
			// endTime = System.currentTimeMillis();
			// System.out.println("Create " + FOLDER_LOOP + " folders in " +
			// (endTime - startTime) + "ms.");
			//
			startTime = System.currentTimeMillis();
			m.listDirWithSha1AndVersionAndChildDirs(token, BASE_PATH);
			endTime = System.currentTimeMillis();
			System.out.println("List all replicated dirs in "
					+ (endTime - startTime) + "ms.");

			m.logout(token);
		} catch (XmlRpcException e) {
			e.printStackTrace();
		}

	}

	private static final int LOOP = 50000;
	private static final int FOLDER_LOOP = 100;
	private static final String BASE_PATH = "/MySpace/testOpt/";
}
