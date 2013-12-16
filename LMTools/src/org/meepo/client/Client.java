package org.meepo.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.meepo.common.ResponseCode;
import org.meepo.fs.AttributeInfo;
import org.meepo.fs.FileDistInfo;
import org.meepo.xmlrpc.AdminRpcInterface;
import org.meepo.xmlrpc.MeepoAssist;
import org.meepo.xmlrpc.RpcInterface;

public class Client {

	public Client(URL url) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setBasicEncoding("UTF-8");
		config.setServerURL(url);
		config.setEnabledForExtensions(true);

		this.xmlRpcClient = new XmlRpcClient();
		this.xmlRpcClient.setConfig(config);
	}

	public static String[] parseCommand(String command) {
		ArrayList<String> segs = new ArrayList<String>();
		StringBuilder segBuilder = new StringBuilder();

		char currentChar;
		for (int i = 0; i < command.length(); i++) {
			currentChar = command.charAt(i);
			if (currentChar == '\\') {
				if (i == command.length() - 1) {
					return null;
				}
				if (command.charAt(i + 1) != ' ') {
					return null;
				}

				segBuilder.append(" ");
				i++;
				continue;
			}

			if (currentChar == ' ') {
				if (segBuilder.length() != 0) {
					segs.add(segBuilder.toString());
				}
				segBuilder = new StringBuilder();
				continue;
			}

			segBuilder.append(currentChar);
		}

		if (segBuilder.length() != 0) {
			segs.add(segBuilder.toString());
		}
		return segs.toArray(new String[0]);
	}

	public static String getSizeString(long size) {
		if (size < KILO_BYTE) {
			return size + "  B";
		} else if (size < MEGA_BYTE) {
			return size / KILO_BYTE + " KB";
		} else if (size < GIGA_BYTE) {
			return size / MEGA_BYTE + " MB";
		} else if (size < TERA_BYTE) {
			return size / GIGA_BYTE + " TB";
		} else {
			return size / TERA_BYTE + " PB";
		}
	}

	protected void login() {
		// Old_TODO:
		Object[] params = new Object[] { this.username, this.password };
		try {
			this.token = (String) this.meepoAdmin.loginWithEmail(username);
			// this.token = (String) this.meepo.login(username, password);

			this.currentPath = "/";
			this.isLogined = true;
			System.out.println("Welcome " + this.username + "!");
			return;
		} catch (XmlRpcException e) {
			this.currentPath = null;
			this.isLogined = false;
			switch (e.code) {
			case ResponseCode.USER_NOT_EXISTS:
				System.out.println("User not exists.");
				return;
			case ResponseCode.PASSWD_INCORRECT:
				System.out.println("Password incorrect.");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	protected void logout() {
		Object[] params = new Object[] { this.token };
		try {
			this.xmlRpcClient.execute("Meepo.logout", params);
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.TOKEN_INCORRECT:
				break;
			default:
				// Old_TODO:
				break;
			}
		} finally {
			this.username = null;
			this.password = null;
			this.token = null;
		}
	}

	protected void showTime() {
		try {
			Long time = this.meepo.getServerTime();
			Date date = new Date(time);
			System.out.println(String.format("Server time: %s, %s.",
					date.toLocaleString(), date.toGMTString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void changeDirectory(String path) {
		if (path.equals(".")) {
			return;
		}
		if (path.equals("..")) {
			if (this.currentPath.equals("/")) {
				return;
			}

			String[] segs = this.currentPath.split("/");
			this.currentPath = this.currentPath.substring(0,
					this.currentPath.length() - segs[segs.length - 1].length()
							- 1);
			return;
		}

		String targetPath = path;
		try {
			if (path.charAt(0) != '/') {
				targetPath = this.currentPath + path;
			}

			// metaStr = (String) this.xmlRpcClient.execute("Meepo.getAttr",
			// params);

			Map<Object, Object> map = this.meepo.getAttrMap(this.token,
					targetPath);

			// Boolean isDir = (Boolean)map.get(MeepoAssist.ATTR_IS_DIR);
			AttributeInfo attrInfo = new AttributeInfo(map);

			// meta = Meta.parseMeta(metaStr);
			if (!attrInfo.isDir) {
				System.out.println("'" + path + "': Not a directory");
				return;
			}
			this.currentPath = this.currentPath + path + "/";
			return;
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + path + "': No such file or directory");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	protected void makeDirectory(String path) {
		if (path.equals(".")) {
			return;
		}
		if (path.equals("..")) {
			return;
		}

		try {
			Object[] params = null;

			if (path.charAt(0) == '/') {
				params = new Object[] { this.token, path };
			} else {
				params = new Object[] { this.token, this.currentPath + path };
			}

			this.xmlRpcClient.execute("Meepo.mkDir", params);
			return;
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_ALREADY_EXISTS:
				System.out.println("'" + path + "': File already exists");
				return;
			case ResponseCode.PARENT_DIR_NOT_EXISTS:
				System.out.println("'" + path
						+ "': Parent directory not exists");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}
	}

	protected void remove(String path) {
		if (path.equals(".")) {
			return;
		}
		if (path.equals("..")) {
			return;
		}

		try {
			Object[] params = null;

			if (path.charAt(0) == '/') {
				params = new Object[] { this.token, path };
			} else {
				params = new Object[] { this.token, this.currentPath + path };
			}

			this.xmlRpcClient.execute("Meepo.delete", params);
			return;
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + path + "': No such file or directory");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}
	}

	protected void move(String srcPath, String dstPath) {
		if (srcPath.equals(".")) {
			return;
		}
		if (srcPath.equals("..")) {
			return;
		}

		try {
			String dstRealPath = dstPath;
			if (dstPath.endsWith("/")) {
				String[] segs = srcPath.split("/");
				dstRealPath += segs[segs.length - 1];
			}

			Object[] params = null;
			if (srcPath.charAt(0) == '/') {
				if (dstPath.charAt(0) == '/') {
					params = new Object[] { this.token, srcPath, dstRealPath };
				} else {
					params = new Object[] { this.token, srcPath,
							this.currentPath + dstRealPath + "/" };
				}
			} else {
				if (dstPath.charAt(0) == '/') {
					params = new Object[] { this.token,
							this.currentPath + srcPath, dstRealPath };
				} else {
					params = new Object[] { this.token,
							this.currentPath + srcPath,
							this.currentPath + dstRealPath };
				}
			}
			this.xmlRpcClient.execute("Meepo.move", params);
			// this.meepo.move(token, oldPath, newPath)
			return;
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + srcPath
						+ "': No such file or directory");
				return;
			case ResponseCode.FILE_DIR_ALREADY_EXISTS:
				System.out.println("'" + dstPath + "': File already exists");
				return;
			case ResponseCode.PARENT_DIR_NOT_EXISTS:
				System.out.println("'" + dstPath
						+ "': Parent directory not exists");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}
	}

	protected void list(String path) {
		try {
			Object[] objs = null;

			if (path.charAt(0) == '/') {
			} else {
				path = this.currentPath + path;
			}

			Map<Object, Object> map = meepo.getAttrMap(this.token, path);
			AttributeInfo attrMap = new AttributeInfo(map);
			System.out.println(attrMap.toString());

			objs = this.meepo.listDirMaps(this.token, path);
			for (int i = 0; i < objs.length; i++) {
				// metas[i] = Meta.parseMeta((String) objs[i]);
				map = (Map<Object, Object>) (objs[i]);
				System.out.println(new AttributeInfo(map).toString());
			}
			// for (Meta meta : metas) {
			// System.out.println(meta.getAcl() + "\t" +
			// getSizeString(meta.getSize()) + "\t" + meta.getName());
			// }
			return;
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + path + "': No such file or directory");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	protected void download(String path) {
		try {
			Object[] objs = null;

			if (path.charAt(0) == '/') {
			} else {
				path = this.currentPath + path;
			}

			Map<Object, Object> map;
			objs = this.meepo.downloadRequest(this.token, path);
			for (int i = 0; i < objs.length; i++) {
				// metas[i] = Meta.parseMeta((String) objs[i]);
				map = (Map<Object, Object>) (objs[i]);
				System.out.println(new FileDistInfo(map).toString());
			}
			// for (Meta meta : metas) {
			// System.out.println(meta.getAcl() + "\t" +
			// getSizeString(meta.getSize()) + "\t" + meta.getName());
			// }
			return;
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + path + "': No such file or directory");
				return;
			default:
				// Old_TODO:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	protected void showCapacity(String path) {
		try {
			Map<Object, Object> map = meepo.getCapacityMap(this.token, path);
			double sizeInGB;
			StringBuffer sb = new StringBuffer();

			sb.append(MeepoAssist.CAPACITY_TOTAL);
			sb.append("\t");
			sizeInGB = (Long) map.get(MeepoAssist.CAPACITY_TOTAL)
					/ (1024.0 * 1024.0 * 1024);
			sb.append(String.format("%.2fGB", sizeInGB));
			sb.append("\t");
			sb.append(MeepoAssist.CAPACITY_USED);
			sb.append("\t");
			sizeInGB = (Long) map.get(MeepoAssist.CAPACITY_USED)
					/ (1024.0 * 1024.0 * 1024);
			sb.append(String.format("%.2fGB", sizeInGB));
			System.out.println(sb.toString());

		} catch (XmlRpcException e) {
			switch (e.code) {
			default:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	protected void getAttr(String remotePath) {
		try {
			Map<Object, Object> map = meepo.getAttrMap(this.token,
					this.currentPath + remotePath);
			AttributeInfo attrMap = new AttributeInfo(map);
			System.out.println(attrMap.toString());
		} catch (XmlRpcException e) {
			System.out.println(e.code + "\n" + e.getMessage());
		}
	}

	protected void restore(String path) {
		try {
			String pp = this.currentPath + path;
			this.meepo.restore(this.token, pp);
		} catch (XmlRpcException e) {
			System.out.println(e.code + "\n" + e.getMessage());
		}
	}

	protected void get(String remotePath, String localPath) {
		if (remotePath.equals(".")) {
			System.out.println("'" + remotePath + "': Not a file");
			return;
		}
		if (remotePath.equals("..")) {
			System.out.println("'" + remotePath + "': Not a file");
			return;
		}

		try {
			Object[] params = null;
			if (remotePath.charAt(0) == '/') {
				params = new Object[] { this.token, remotePath };
			} else {
				params = new Object[] { this.token,
						this.currentPath + remotePath };
			}

			String metaStr = (String) this.xmlRpcClient.execute(
					"Meepo.getAttr", params);
			// if (Meta.parseMeta(metaStr).isDirectory()) {
			// System.out.println("'" + remotePath + "': Not a file");
			// return;
			// }
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + remotePath
						+ "': No such file or directory");
				return;
			default:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}

		Object[] retObjects = null;
		String path = null;
		try {
			Object[] params = null;
			if (remotePath.charAt(0) == '/') {
				params = new Object[] { this.token, remotePath };
			} else {
				params = new Object[] { this.token,
						this.currentPath + remotePath };
			}
			retObjects = (Object[]) this.xmlRpcClient.execute(
					"Meepo.readFileEncrypted", params);
			path = (String) retObjects[0] + (String) retObjects[1];
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + remotePath
						+ "': No such file or directory");
				return;
			default:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}

		URL url = null;
		try {
			url = new URL(path);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.exit(1);
		}

		long startTime = 0;
		long endTime = 0;
		int totalBytes = 0;
		try {
			startTime = new Date().getTime();

			FileOutputStream fileOutput = new FileOutputStream(localPath);

			HttpURLConnection urlConnection = (HttpURLConnection) url
					.openConnection();
			urlConnection.setRequestMethod("GET");
			urlConnection.setDoOutput(false);
			urlConnection.setDoInput(true);
			urlConnection.setUseCaches(false);
			InputStream netInput = urlConnection.getInputStream();

			System.out.println("Please wait for download to complete...");

			byte[] dataBytes = null;
			if (netInput.available() > MAX_BUFFER_SIZE) {
				dataBytes = new byte[MAX_BUFFER_SIZE];
			} else {
				dataBytes = new byte[netInput.available()];
			}

			int numberOfBytesRead = 0;
			while ((numberOfBytesRead = netInput.read(dataBytes)) > 0) {
				fileOutput.write(dataBytes, 0, numberOfBytesRead);
				totalBytes += numberOfBytesRead;
				System.out.print("  Downloaded " + totalBytes + " bytes \r");
			}
			fileOutput.close();
			netInput.close();
			System.out.println();

			endTime = new Date().getTime();
			System.out.println("Succeeded get '" + remotePath + "' to '"
					+ localPath + "': " + totalBytes + " bytes in "
					+ (endTime - startTime) + " ms");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	// Old_TODO:
	protected void put(String localPath, String remotePath) {
		if (remotePath.equals(".")) {
			System.out.println("'" + remotePath + "': Not a file");
			return;
		}
		if (remotePath.equals("..")) {
			System.out.println("'" + remotePath + "': Not a file");
			return;
		}

		try {
			Object[] params = null;
			if (remotePath.charAt(0) == '/') {
				params = new Object[] { this.token, remotePath };
			} else {
				params = new Object[] { this.token,
						this.currentPath + remotePath };
			}
			this.xmlRpcClient.execute("Meepo.mkFile", params);
			// path = (String) retObjects[0] + (String) retObjects[1];
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_ALREADY_EXISTS:
				System.out.println("'" + remotePath + "': File already exists");
				return;
			case ResponseCode.PARENT_DIR_NOT_EXISTS:
				System.out.println("'" + remotePath
						+ "': Parent directory not exists");
				return;
			default:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}

		Object[] retObjects = null;
		String path = null;
		try {
			Object[] params = null;
			if (remotePath.charAt(0) == '/') {
				params = new Object[] { this.token, remotePath, 0 };
			} else {
				params = new Object[] { this.token,
						this.currentPath + remotePath, 0 };
			}
			retObjects = (Object[]) this.xmlRpcClient.execute(
					"Meepo.writeFileEncrypted", params);
			path = (String) retObjects[0] + (String) retObjects[1];
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_ALREADY_EXISTS:
				System.out.println("'" + remotePath + "': File already exists");
				return;
			case ResponseCode.PARENT_DIR_NOT_EXISTS:
				System.out.println("'" + remotePath
						+ "': Parent directory not exists");
				return;
			default:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}

		URL url = null;
		try {
			url = new URL(path);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.exit(1);
		}

		long startTime = 0;
		long endTime = 0;
		int totalBytes = 0;
		try {
			startTime = new Date().getTime();

			FileInputStream fileInput = new FileInputStream(localPath);

			String lineEnd = "\r\n";
			String twoHyphens = "--";
			String boundary = "*****";

			HttpURLConnection urlConnection = (HttpURLConnection) url
					.openConnection();
			urlConnection.setRequestMethod("POST");
			urlConnection.setDoOutput(true);
			urlConnection.setDoInput(true);
			urlConnection.setUseCaches(false);
			urlConnection.setRequestProperty("Connection", "Keep-Alive");
			urlConnection.setRequestProperty("Content-Type",
					"multipart/form-data;boundary=" + boundary);
			urlConnection.setRequestProperty("Z-size", "100");
			OutputStream netOutput = urlConnection.getOutputStream();
			DataOutputStream netDataOutput = new DataOutputStream(netOutput);

			netDataOutput.writeBytes(twoHyphens + boundary + lineEnd);
			netDataOutput
					.writeBytes("Content-Disposition: form-data; name=\"upload\";"
							+ " filename=\"" + path + "\"" + lineEnd);
			netDataOutput.writeBytes(lineEnd);

			System.out.println("Please wait for upload to complete...");

			byte[] dataBytes = null;
			// 10M
			if (fileInput.available() > MAX_BUFFER_SIZE) {
				dataBytes = new byte[MAX_BUFFER_SIZE];
			} else {
				dataBytes = new byte[fileInput.available()];
			}

			int numberOfBytesRead = 0;
			while ((numberOfBytesRead = fileInput.read(dataBytes)) > 0) {
				netDataOutput.write(dataBytes, 0, numberOfBytesRead);
				totalBytes += numberOfBytesRead;
				System.out.print("  Uploaded " + totalBytes + " bytes \r");
			}
			netDataOutput.writeBytes(lineEnd);
			netDataOutput.writeBytes(twoHyphens + boundary + twoHyphens
					+ lineEnd);

			fileInput.close();
			netDataOutput.flush();
			netDataOutput.close();

			System.out.println();

			DataInputStream inStream = new DataInputStream(
					urlConnection.getInputStream());
			String str;
			while ((str = inStream.readLine()) != null) {
				System.out.println("Server response is: " + str);
				System.out.println("");
			}
			inStream.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Cannot get '" + remotePath + "' to '"
					+ localPath + "': IOException");
			return;
		}

		try {
			Object[] params = null;
			if (remotePath.charAt(0) == '/') {
				params = new Object[] { this.token, remotePath, totalBytes };
			} else {
				params = new Object[] { this.token,
						this.currentPath + remotePath, totalBytes };
			}
			this.xmlRpcClient.execute("Meepo.writeConfirm", params);
			endTime = new Date().getTime();
			System.out.println("Succeeded put '" + localPath + "' to '"
					+ remotePath + "': " + totalBytes + " bytes in "
					+ (endTime - startTime) + " ms");
		} catch (XmlRpcException e) {
			switch (e.code) {
			case ResponseCode.FILE_DIR_NOT_EXISTS:
				System.out.println("'" + remotePath
						+ "': No such file or directory");
				return;
			default:
				System.out.println("System error " + e.getMessage() + " "
						+ e.code);
				System.exit(1);
			}
		}
	}

	public static void main(String[] args) {
		System.out
				.println("Welcome to Meepo! Ver 1.0 beta. Copyright H.P.C Lab. Tsinghua U.");

		BufferedReader stdin = new BufferedReader(new InputStreamReader(
				System.in));
		String serverAddr = null;
		if (args.length == 0) {
			System.out.print("Server address: ");
			try {
				serverAddr = stdin.readLine();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
			serverAddr = args[0];
		}
		// if (!serverAddr.endsWith(":8080")) {
		// serverAddr = serverAddr + ":8080";
		// }

		if (serverAddr.isEmpty()) {
			serverAddr = serverURL;
		}

		URL url = null;
		try {
			url = new URL(serverAddr);
		} catch (MalformedURLException e) {
			System.out.println("Invalid server url.");
			System.exit(1);
		}

		Client client = new Client(url);

		try {
			System.out.print("Username: ");
			String s = stdin.readLine();
			if (!s.isEmpty()) {
				client.username = s;
			}
			System.out.print("Password: ");
			s = stdin.readLine();
			if (!s.isEmpty()) {
				client.password = s;
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		client.login();

		String command = null;
		while (true) {
			System.out.print("> ");

			try {
				command = stdin.readLine();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

			String[] segs = parseCommand(command);
			if (segs == null) {
				System.out.println("Illegal command format.");
				continue;
			}
			if (segs.length == 0) {
				continue;
			}

			String operation = segs[0];
			if (!client.isLogined) {
				System.out.println("Please login first.");
				// Old_TODO:
				continue;
			}

			if (operation.equals("ls") || operation.equals("dir")) {
				// List
				if (segs.length == 1) {
					client.list(client.currentPath);
				} else if (segs.length == 2) {
					client.list(segs[1]);
				} else {
					System.out.println("Illegal command format.");
				}

			} else if (operation.equals("getattr")) {
				if (segs.length == 2) {
					client.getAttr(segs[1]);
				} else {
					System.out.println("Illegal command format.");
				}
			} else if (operation.equals("download")) {
				if (segs.length == 2) {
					client.download(segs[1]);
				} else {
					System.out.println("Illegal command format.");
				}

			} else if (operation.equals("capacity")) {
				// Show capacity infomation
				if (segs.length == 1) {
					client.showCapacity(client.currentPath);
				} else if (segs.length == 2) {
					client.showCapacity(segs[1]);
				} else {
					System.out.println("Illegal command format.");
				}

			} else if (operation.equals("cd")) {
				// Change directory
				if (segs.length != 2) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.changeDirectory(segs[1]);

			} else if (operation.equals("pwd")) {
				// Print work directory
				if (segs.length != 1) {
					System.out.println("Illegal command format.");
					continue;
				}
				System.out.println(client.currentPath);

			} else if (operation.equals("mkdir")) {
				// Make directory
				if (segs.length != 2) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.makeDirectory(segs[1]);

			} else if (operation.equals("rm") || operation.equals("del")) {
				// Remove file or directory
				if (segs.length != 2) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.remove(segs[1]);

			} else if (operation.equals("mv")) {
				// Move file
				if (segs.length != 3) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.move(segs[1], segs[2]);

			} else if (operation.equals("get")) {
				// Get file
				if (segs.length != 3) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.get(segs[1], segs[2]);

			} else if (operation.equals("put")) {
				// Put file
				if (segs.length != 3) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.put(segs[1], segs[2]);

			} else if (operation.equals("exit") || operation.equals("quit")) {
				// Quit
				if (segs.length != 1) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.logout();
				System.exit(0);

			} else if (operation.equals("time")) {
				// Get server time
				client.showTime();
			} else if (operation.equals("restore")) {
				if (segs.length != 2) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.restore(segs[1]);
			} else if (operation.equals("help")) {
				// Get help information

			} else if (operation.equals("route")) {
				// get which hyla node the user belongs to
				if (segs.length != 2) {
					System.out.println("Illegal command format.");
					continue;
				}
				client.route(segs[1]);

			} else {
				// Unknown operation
				System.out
						.println("Unknown operation, enter 'help' to get help information.");
			}
		}
	}

	protected void route(String email) {
		String r = meepoAdmin.getUserRoute(email);
		System.out.println(r);
	}

	public static final long KILO_BYTE = 1024L;
	public static final long MEGA_BYTE = 1024L * KILO_BYTE;
	public static final long GIGA_BYTE = 1024L * MEGA_BYTE;
	public static final long TERA_BYTE = 1024L * GIGA_BYTE;
	public static final long PETA_BYTE = 1024L * TERA_BYTE;

	public static final int MAX_BUFFER_SIZE = 10 * 1024 * 1024;

	private XmlRpcClient xmlRpcClient = null;
	private AdminRpcInterface meepoAdmin = RpcProxyFactory.getInstance()
			.getMeepoAdminImpl(serverURL);
	private RpcInterface meepo = RpcProxyFactory.getInstance().getMeepoImpl(
			serverURL);

	// private String username = "i@ztrix.me";
	// private String username = "madnuj@gmail.com";
	// private String username = "test1@meepo.org";
	// private String username = "msmummy@qq.com";
	private String password = "ms654312";
	// private String password = "tttttt";
	// private String password = "112211";
	// private String username = "chenkang@tsinghua.edu.cn";
	// private String username = "wuyw@tsinghua.edu.cn";
	// private String username = "MaomengSu@163.com";
	// private String password = "bisheng1989";
	// private String username = "pin.gao2008@gmail.com";
	private String username = "msmummy@gmail.com";

	// private String username = "asdf@xx.org";
	// private String password = "hpcgrid";

	// private String username = "wangqiuping816@gmail.com";
	// private String password = "huaisha";
	// private String username = "canjian231@163.com";
	// private String password = "123456";
	// private String username = "ck99@mails.tsinghua.edu.cn";
	// private String password = "asdf1234";
	private boolean isLogined = false;
	private String token = null;
	private String currentPath = null;

	// private static String serverURL = "http://127.0.0.1:8080/meepo/xmlrpc";
	private static String serverURL = "http://lm1.thu.meepo.org:8080/meepo/xmlrpc";

	// private static String serverURL =
	// "http://lm1.hfut.meepo.org:80/meepo/xmlrpc";
	// private static String serverURL =
	// "http://lm1.mi.meepo.org:8080/meepo/xmlrpc";
	// private String serverURL = "http://lm2.meepo.org:8080/meepo/xmlrpc";

}
