package org.meepo.test.client;

import java.net.URL;

import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

/**
 * This is a sample client for test, notice that you should probably change the
 * Server URL and port
 * 
 * @author MS
 * 
 */

public class MeepoAddTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setBasicEncoding("UTF-8");
		try {
			Object[] params;
			// final URL url = new
			// URL("http://meepo.thuhpc.org:8080/Meepo/xmlrpc");
			// final URL url = new URL("http://127.0.0.1:8080/Meepo/xmlrpc");
			final URL url = new URL("http://10.0.21.254:8080/Meepo/xmlrpc");
			long startTime;
			long endTime;
			XmlRpcClient client = new XmlRpcClient();
			config.setServerURL(url);
			config.setEnabledForExtensions(true);
			client.setConfig(config);

			// Try to do Add with single client for once;
			startTime = System.currentTimeMillis();
			params = new Object[] { new Integer(33), new Integer(9) };
			int ret = (Integer) client.execute("Meepo.add", params);
			endTime = System.currentTimeMillis();
			System.out.println("Do Add once with single client in "
					+ (endTime - startTime) + "ms.");

			// Try to do Add with single client for once;
			client = new XmlRpcClient();
			config.setServerURL(url);
			config.setEnabledForExtensions(true);
			client.setConfig(config);
			startTime = System.currentTimeMillis();
			params = new Object[] { new Integer(3), new Integer(9) };
			ret = (Integer) client.execute("Meepo.add", params);
			endTime = System.currentTimeMillis();
			System.out.println("Do Add once with single client in "
					+ (endTime - startTime) + "ms.");

			// Try to do Add with single client for once;
			client = new XmlRpcClient();
			config.setServerURL(url);
			config.setEnabledForExtensions(true);
			client.setConfig(config);
			startTime = System.currentTimeMillis();
			params = new Object[] { new Integer(7), new Integer(9) };
			ret = (Integer) client.execute("Meepo.add", params);
			endTime = System.currentTimeMillis();
			System.out.println("Do Add once with single client in "
					+ (endTime - startTime) + "ms.");

			// //Try to do Add with 500 client for a thousand times;
			// startTime = System.currentTimeMillis();
			// ArrayList<Thread> threads = new ArrayList<Thread>();
			// for(int i=0; i<=500; i++){
			// final int ii = i;
			// Thread thread = new Thread(){
			// public void run(){
			// XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
			// XmlRpcClient client = new XmlRpcClient();
			// config.setServerURL(url);
			// client.setConfig(config);
			// try {
			// client.execute("Meepo.add", new Object[]{new Integer(ii), new
			// Integer(ii)});
			// client.execute("Meepo.add", new Object[]{new Integer(ii), new
			// Integer(ii)});
			// } catch (XmlRpcException e) {
			// System.out.println("Sys Err when add " + ii);
			// e.printStackTrace();
			// }
			// }
			// };
			// threads.add(thread);
			// thread.start();
			// }
			// for(Thread t:threads){
			// t.join();
			// }
			// endTime = System.currentTimeMillis();
			// System.out.println("Do Add a thousand times with 500 clients in "+(endTime
			// - startTime)+"ms.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
