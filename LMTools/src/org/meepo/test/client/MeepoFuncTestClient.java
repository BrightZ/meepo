package org.meepo.test.client;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

/**
 * This is a sample client for test, notice that you should probably change the
 * Server URL and port
 * 
 * @author MS
 * 
 */

public class MeepoFuncTestClient {
	public static void main(String[] args) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setBasicEncoding("UTF-8");
		try {
			Object[] params;
			// final URL url = new
			// URL("http://meepo.thuhpc.org:8080/Meepo/xmlrpc");
			final URL url = new URL("http://127.0.0.1:8080/Meepo/xmlrpc");
			// final URL url = new URL("http://10.0.1.229:8080/Meepo/xmlrpc");
			int ret;
			long startTime;
			long endTime;
			XmlRpcClient client = new XmlRpcClient();

			config.setServerURL(url);
			config.setEnabledForExtensions(true);
			client.setConfig(config);

			params = new Object[] { "madnuj@gmail.com", "112233" };
			// params = new Object[]{"msmummy@gmail.com", "654312"};
			String token = (String) client.execute("Meepo.login", params);
			System.out.println("token:" + token);

			try {
				// Try to delete all those things if exist;
				params = new Object[] { token, BASE_PATH };
				ret = (Integer) client.execute("Meepo.rmDir", params);
				System.out.println(ret);
			} catch (XmlRpcException e) {
			}

			// //Try to create a file with single client
			// startTime = System.currentTimeMillis();
			// String filename = "/MySpace/testFile";
			// client.execute("Meepo.mkFile", new Object[]{token, filename});
			// endTime = System.currentTimeMillis();
			// System.out.println("Create a files with a single clients in "+(endTime
			// - startTime)+"ms.");
			//
			// //Try to delete the file above
			// startTime = System.currentTimeMillis();
			// filename = "/MySpace/testFile";
			// client.execute("Meepo.rmFile", new Object[]{token, filename});
			// endTime = System.currentTimeMillis();
			// System.out.println("Delete a files with a single clients in "+(endTime
			// - startTime)+"ms.");
			//
			// Create Dir /MySpace/testDir1/
			// Create Dir /MySpace/testDir1/testDir2/
			// Create File /MySpace/testDir1/testFile1
			// Create File /MySpace/testDir1/testDir2/testFile2
			params = new Object[] { token, BASE_PATH };
			ret = (Integer) client.execute("Meepo.mkDir", params);
			System.out.println(ret);

			params = new Object[] { token, BASE_PATH + "testDir2/" };
			ret = (Integer) client.execute("Meepo.mkDir", params);
			System.out.println(ret);

			params = new Object[] { token, BASE_PATH + "testFile1" };
			ret = (Integer) client.execute("Meepo.mkFile", params);
			System.out.println(ret);

			// Try to get attribute of /MySpace/testDir1/testFile1
			System.out
					.println("Try to get attribute of /MySpace/testDir1/testFile1");
			params = new Object[] { token, BASE_PATH + "testFile1" };
			System.out
					.println((String) client.execute("Meepo.getAttr", params));

			params = new Object[] { token, BASE_PATH + "testFile3" };
			ret = (Integer) client.execute("Meepo.mkFile", params);
			System.out.println(ret);

			params = new Object[] { token, BASE_PATH + "testDir2/testFile2" };
			ret = (Integer) client.execute("Meepo.mkFile", params);
			System.out.println(ret);

			// Delete File /MySpace/testDir1/testDir2/testFile2
			params = new Object[] { token, BASE_PATH + "testDir2/testFile2" };
			ret = (Integer) client.execute("Meepo.delete", params);
			System.out.println(ret);

			// List Dir /MySpace/testDir1/testDir2
			params = new Object[] { token, BASE_PATH + "testDir2" };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}

			// List Dir /MySpace/testDir1/testDir2
			System.out.println("List /MySpace with sha1 and version:");
			params = new Object[] { token, "/MySpace" };
			for (Object s : (Object[]) client.execute(
					"Meepo.listDirWithSha1AndVersion", params)) {
				System.out.println(s);
			}

			// List Dir /MySpace/testDir1/testDir2
			System.out.println("List with sha1 and version:");
			params = new Object[] { token, "/MySpace" };
			for (Object s : (Object[]) client.execute(
					"Meepo.listDirWithSha1AndVersionAndChildDirs", params)) {
				System.out.println(s);
			}

			// TEST For move, aka rename

			params = new Object[] { token, BASE_PATH + "testFile3",
					BASE_PATH + "testFile4" };
			ret = (Integer) client.execute("Meepo.move", params);
			System.out.println(ret);

			params = new Object[] { token, BASE_PATH + "testFile1",
					BASE_PATH + "testDir2/testFile1" };
			ret = (Integer) client.execute("Meepo.move", params);
			System.out.println(ret);

			params = new Object[] { token, BASE_PATH + "wang" };
			ret = (Integer) client.execute("Meepo.mkFile", params);
			System.out.println(ret);

			params = new Object[] { token, BASE_PATH + "wang",
					BASE_PATH + "qiuping" };
			ret = (Integer) client.execute("Meepo.move", params);
			System.out.println(ret);

			System.out.println("Listing after move.");
			System.out.println("Listing /MySpace/testDir1.");
			// List Dir /MySpace/testDir1
			params = new Object[] { token, BASE_PATH };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}
			System.out.println("Listing /MySpace/testDir1/testDir2.");
			// List Dir /MySpace/testDir1/testDir2
			params = new Object[] { token, BASE_PATH + "testDir2" };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}

			// List Root
			// retrun is MySpace:[MySpace_zh-CN]:[d or
			// f]:[ctime]:[mtime]:[atime]:[size]
			System.out.println("Trying to List Root");
			params = new Object[] { token, "zh-CN" };
			for (Object s : (Object[]) client.execute("Meepo.listRoot", params)) {
				System.out.println(s);
			}

			// Trying to list /
			System.out.println("Trying to list /");
			params = new Object[] { token, "/" };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}

			// List Dir /MySpace/testDir1/
			params = new Object[] { token, BASE_PATH };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}

			// Set Attr of /MySpace/testDir1/testFile1, now only size of bytes;
			String cTimeInString = 0 + "";
			String mTimeInString = 0 + "";
			String aTimeInString = 0 + "";
			String sizeInString = 256L + "";
			params = new Object[] { token, BASE_PATH + "testDir2/testFile1",
					sizeInString, cTimeInString, mTimeInString, aTimeInString };
			client.execute("Meepo.setAttr", params);

			// Get Attr of ROOT, isDir, size, ctime, mtime, atime

			// Object[] retObjs;
			String attr;
			params = new Object[] { token, "/" };
			attr = (String) client.execute("Meepo.getAttr", params);
			System.out.println(attr);

			// Get Attr of /MySpace/, isDir, size, ctime, mtime, atime
			params = new Object[] { token, "/MySpace/" };
			attr = (String) client.execute("Meepo.getAttr", params);
			System.out.println(attr);

			// Get Attr of /MySpace
			params = new Object[] { token, "/MySpace" };
			attr = (String) client.execute("Meepo.getAttr", params);
			System.out.println(attr);

			// Try to write the file /MySpace/testDir1/testDir2/testFile1
			System.out.println("Try to write the encrypted path:"
					+ "/MySpace/testDir1/testDir2/testFile1");
			params = new Object[] { token,
					"/MySpace/testDir1/testDir2/testFile1", 0 };
			for (Object o : (Object[]) client.execute(
					"Meepo.writeFileEncrypted", params)) {
				System.out.println(o);
			}

			// Try to write the file /MySpace/2G
			// System.out.println("Try to write the encrypted path:" +
			// "/MySpace/2G");
			// params = new Object[]{token, "/MySpace/2G", 0};
			// for(Object o : (Object
			// [])client.execute("Meepo.writeFileEncrypted", params)) {
			// System.out.println(o);
			// }

			// Try to read the file /MySpace/testDir1/testDir2/testFile1
			params = new Object[] { token,
					"/MySpace/testDir1/testDir2/testFile1" };
			attr = (String) client.execute("Meepo.readFile", params);
			System.out.println("Try to read the path:" + attr);

			// Try to read the file /MySpace/testDir1/testFile4
			params = new Object[] { token, "/MySpace/testDir1/testFile4" };
			attr = (String) client.execute("Meepo.readFile", params);
			System.out.println("Try to read /MySpace/testDir1/testFile4 :"
					+ attr);

			// Try to get attribute of not existing file.
			System.out.println("Try to get attr of /group:");
			params = new Object[] { token, "/group" };
			attr = (String) client.execute("Meepo.getAttr", params);
			System.out.println(attr);

			// Try to list /group
			System.out.println("Try to list /group:" + attr);
			params = new Object[] { token, "/group" };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}

			// Try to list /public
			System.out.println("Try to list /public:" + attr);
			params = new Object[] { token, "/public" };
			for (Object s : (Object[]) client.execute("Meepo.listDir", params)) {
				System.out.println(s);
			}

			// Try to write the file /MySpace/testDir1/testFile4
			params = new Object[] { token, "/MySpace/testDir1/testFile4", 0 };
			attr = (String) client.execute("Meepo.writeFile", params);
			System.out.println("Try to read /MySpace/testDir1/testFile4 :"
					+ attr);

			// Try to writeConfirm with file size the file
			// /MySpace/testDir1/testFile4
			params = new Object[] { token, "/MySpace/testDir1/testFile4",
					"1024" };
			client.execute("Meepo.writeConfirm", params);

			// Try to write the file /MySpace/testDir1/testFile4
			params = new Object[] { token, "/MySpace/testDir1/testFile4", 0 };
			attr = (String) client.execute("Meepo.writeFile", params);
			System.out.println("Try to read /MySpace/testDir1/testFile4 :"
					+ attr);

			// Try to writeConfirmWithSha1AndVersion the file
			// /MySpace/testDir1/testFile4
			params = new Object[] { token, "/MySpace/testDir1/testFile4",
					"1024", "TESTSHA1", 5 };
			client.execute("Meepo.writeConfirmWithSha1AndVersion", params);

			// Try to getSha1AndVersion the file /MySpace/testDir1/testFile4
			params = new Object[] { token, "/MySpace/testDir1/testFile4" };
			for (Object o : (Object[]) client.execute(
					"Meepo.getSha1AndVersion", params)) {
				System.out.println(o);
			}

			// Try to delete all those things above;
			params = new Object[] { token, "/MySpace/testDir1/" };
			ret = (Integer) client.execute("Meepo.rmDir", params);
			System.out.println(ret);

			// Try to Logout
			params = new Object[] { token };
			ret = (Integer) client.execute("Meepo.logout", params);
			System.out.println(ret);
		} catch (XmlRpcException e) {
			System.out.println(e.code);
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	public static final String BASE_PATH = "/MySpace/testDir1/";
}
