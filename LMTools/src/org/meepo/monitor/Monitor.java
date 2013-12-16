package org.meepo.monitor;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.meepo.config.Environment;
import org.meepo.fs.FileIterator;
import org.meepo.hyla.FileObject;
import org.meepo.hyla.dist.DataSegment;
import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.storage.Storage;
import org.meepo.xmlrpc.MeepoAssist;

public class Monitor {
	private Monitor() {

	}

	public void startInNewThread() {

		MonitorThread mt = new MonitorThread();
		mt.setDaemon(true);
		mt.start();
	}

	private class MonitorThread extends Thread {
		public void run() {
			logger.info("Monitor thread starts.");
			while (true) {
				try {
					FileObject root = MeepoAssist.getInstance()
							.getHylaFileSystem().openObject("/");
					FileIterator fi = new FileIterator(root);
					while (fi.hasNext()) {
						FileObject fo = fi.next();
						if (fo == null) {
							// It could be null, ask frog for why.
							continue;
						}
						if (fo.isDirectory()) {
							continue;
						}
						String path = fo.getPath();
						int replica = 2;

						if (Environment.stingy) {
							if (path.startsWith(MeepoAssist.MYSPACE_PREFIX)) {
								replica = 2;
							} else {
								replica = 1;
							}
						}

						Distribution dst;
						dst = fo.getFileDistribution();
						if (dst == null) {
							return;
						}

						DataSegment[] dsg = dst.getDataSegments();
						Storage storage = MeepoAssist.getInstance()
								.getHylaFileSystem()
								.getStorage(dsg[0].getStorageId());
						if (storage == null) {
							return;
						}
						String base = storage.getURL();
						String filePath = MeepoAssist.SLASH
								+ dsg[0].getPathOnStorage();
						String url = base + filePath;
						setReplica(url, replica);

						// String[] tmp = filePath.split("/");
						// String chunkID = tmp[tmp.length - 1];
						// Chunk chunk = new Chunk(chunkID, replica, path);
						// CassandraClient.getInstance().putChunk(chunk);
						sleep(100);
					}
					sleep(5 * 60 * 1000);
				} catch (InterruptedException e) {
					logger.error("", e);
					break;
				} catch (Exception e) {
					logger.error("", e);
				}
			}
			logger.info("Monitor thread ends.");
		}
	}

	public boolean setReplica(String urlString, int replicaNumber) {
		boolean ret = false;
		URL url;
		try {
			url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Z-Rep", replicaNumber + "");
			conn.setRequestProperty("Z-Token", "BBBBB");
			conn.connect();
			int code = conn.getResponseCode();
			// System.out.println(code);
			// logger.debug(String.format("Setting replica %s", url));
			conn.disconnect();
			ret = true;
		} catch (MalformedURLException e) {
			// logger.error("", e);
		} catch (IOException e) {
			// logger.error("", e);
			// logger.error("");
		}
		return ret;
	}

	public static void main(String[] args) {
		// System.out.println(String.format("%d&", 1));
		for (long i = 0; true; i++) {
			String urlString = "http://ss1.thu.meepo.org/fcgi-bin/fs/2011/12/1/7/6500000200000037";
			Monitor.getInstance().setReplica(urlString, 1);
			System.out.println(i);
		}
	}

	public static Monitor getInstance() {
		return MonitorHolder.instance;
	}

	private static class MonitorHolder {
		private static Monitor instance = new Monitor();
	}

	private static Logger logger = Logger.getLogger(Monitor.class);
}
