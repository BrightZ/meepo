package org.meepo.fs;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.meepo.MeepoExtMeta;
import org.meepo.common.ResponseCode;
import org.meepo.hyla.FileObject;
import org.meepo.xmlrpc.MeepoAssist;

public class PermissionFixer {

	private PermissionFixer() {
		workThread.start();
	}

	public void addDir(FileObject dir) {
		taskQueue.add(dir);
	}

	public int getTaskCount() {
		return taskQueue.size();
	}

	public int getPermission(FileObject o) {
		try {
			MeepoExtMeta extMeta = (MeepoExtMeta) o.getExtendedMeta();
			if (extMeta == null) {
				extMeta = new MeepoExtMeta();
				o.putExtendedMeta(extMeta);
			}
			int accessMask = extMeta.getPermission();
			return accessMask;
		} catch (IOException e) {
			return ResponseCode.ACCESS_READ_ONLY;
		}
	}

	public boolean setPermission(FileObject o, int accessMask) {
		try {
			MeepoExtMeta extMeta = (MeepoExtMeta) o.getExtendedMeta();
			if (extMeta == null) {
				extMeta = new MeepoExtMeta();
			}
			extMeta.setPermission(accessMask);
			o.putExtendedMeta(extMeta);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	public void fixAllGroupDirs() {
		FileObject groupDir = MeepoAssist.getInstance().getHylaFileSystem()
				.openObject(MeepoAssist.GROUP_PREFIX_SUFFIX);
		FileObject publicDir = MeepoAssist.getInstance().getHylaFileSystem()
				.openObject(MeepoAssist.PUBLIC_PREFIX_SUFFIX);

		FileObject[] gpDirs;
		try {
			FileObject[] groupDirs = groupDir.list();
			FileObject[] publicDirs = publicDir.list();
			gpDirs = new FileObject[groupDirs.length + publicDirs.length];
			System.arraycopy(groupDirs, 0, gpDirs, 0, groupDirs.length);
			System.arraycopy(publicDirs, 0, gpDirs, groupDirs.length,
					publicDirs.length);

			for (FileObject gpDir : gpDirs) {
				FileObject[] dirs = gpDir.list();
				for (FileObject dir : dirs) {
					if (dir.getName().equalsIgnoreCase("upload")) {
						this.setPermission(dir, ResponseCode.ACCESS_WRITE_ALLOW);
					}
					this.addDir(dir);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class WorkThread extends Thread {
		public WorkThread() {
			this.setDaemon(true);
		}

		@Override
		public void run() {
			while (true) {
				try {
					FileObject fo = taskQueue.poll();
					if (fo == null) {
						Thread.sleep(SLEEP_TIME_MILLIS);
					} else {
						int accessMask = getPermission(fo);
						FileIterator fi = new FileIterator(fo);
						while (fi.hasNext()) {
							FileObject o = fi.next();
							if (o == null || !o.isDirectory()) {
								continue;
							}
							// Old_TODO Set Permission same as fo
							setPermission(o, accessMask);
						}
					}

				} catch (Exception e) {
					logger.info("Error in permission fixer", e);
				}
			}

		}
	}

	private ConcurrentLinkedQueue<FileObject> taskQueue = new ConcurrentLinkedQueue<FileObject>();
	private WorkThread workThread = new WorkThread();

	private static class PermissionFixerHolder {
		private static PermissionFixer instance = new PermissionFixer();
	}

	public static PermissionFixer getInstance() {
		return PermissionFixerHolder.instance;
	}

	private static int SLEEP_TIME_MILLIS = 5000;
	private static Logger logger = Logger.getLogger(PermissionFixer.class);
}
