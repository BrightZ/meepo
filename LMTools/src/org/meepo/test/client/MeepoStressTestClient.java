package org.meepo.test.client;

import java.net.URL;
import java.util.ArrayList;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.meepo.common.ResponseCode;

/**
 * This is a sample client for test, notice that you should probably change the
 * Server URL and port
 * 
 * @author MS
 * 
 */

public class MeepoStressTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setBasicEncoding("UTF-8");
		try {
			Object[] params;
			// final URL url = new
			// URL("http://meepo.thuhpc.org:8080/Meepo/xmlrpc");
			final URL url = new URL("http://127.0.0.1:8080/Meepo/xmlrpc");
			// final URL url = new URL("http://10.0.1.229:8080/Meepo/xmlrpc");
			long startTime;
			long endTime;
			XmlRpcClient client = new XmlRpcClient();

			config.setServerURL(url);
			config.setEnabledForExtensions(true);
			client.setConfig(config);

			// //Try to do Add with single client for once;
			// startTime = System.currentTimeMillis();
			// params = new Object[]{new Integer(33), new Integer(9)};
			// ret = (Integer) client.execute("Meepo.add", params);
			// endTime = System.currentTimeMillis();
			// System.out.println("Do Add once with single client in "+(endTime
			// - startTime)+"ms.");
			//
			// //Try to do Add with 500 client for a thousand times;
			// startTime = System.currentTimeMillis();
			ArrayList<Thread> threads = new ArrayList<Thread>();
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
			//
			// Try to Login
			params = new Object[] { "msmummy@gmail.com", "654312" };
			String token = (String) client.execute("Meepo.login", params);
			System.out.println("token:" + token);

			// Try to create 400 files;
			startTime = System.currentTimeMillis();
			threads.clear();
			for (int i = 0; i < 200; i++) {
				final String t = token;
				final int ii = i;
				Thread thread = new Thread() {
					public void run() {
						XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
						XmlRpcClient client = new XmlRpcClient();
						config.setServerURL(url);
						client.setConfig(config);
						String filename = "/private/testManyFile" + ii;
						try {
							client.execute("Meepo.mkFile", new Object[] { t,
									filename });
							filename = "/private/testManyFile" + (ii + 500);
							client.execute("Meepo.mkFile", new Object[] { t,
									filename });

							// String addr =
							// (String)client.execute("Meepo.readFile", new
							// Object[]{t, filename});
							// System.out.println("Read " + filename + " :" +
							// addr);
						} catch (XmlRpcException e) {
							System.out.println("Error when creating "
									+ filename);
							e.printStackTrace();
							if (e.code == ResponseCode.SYSTEM_ERROR)
								System.exit(0);
						}
					}
				};
				threads.add(thread);
				thread.start();
			}
			for (Thread t : threads) {
				t.join();
			}
			endTime = System.currentTimeMillis();
			System.out.println("Create a thousand files with 500 clients in "
					+ (endTime - startTime) + "ms.");

			// //Try to delete those files
			// startTime = System.currentTimeMillis();
			// threads.clear();
			// for(int i=0; i<20; i++){
			// final String t = token;
			// final int ii = i;
			// Thread thread = new Thread(){
			// public void run(){
			// XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
			// XmlRpcClient client = new XmlRpcClient();
			// config.setServerURL(url);
			// client.setConfig(config);
			// String filename = "/private/testManyFile"+ii;
			// try {
			// client.execute("Meepo.rmFile", new Object[]{t, filename});
			// filename = "/private/testManyFile" + (ii+500);
			// client.execute("Meepo.rmFile", new Object[]{t, filename});
			// } catch (XmlRpcException e) {
			// System.out.println("Error when removing " + filename);
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
			// System.out.println("Remove a thousand files with 500 clients in "+(endTime
			// - startTime)+"ms.");

			// Try to Logout
			params = new Object[] { token };
			System.out
					.println((Integer) client.execute("Meepo.logout", params));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
