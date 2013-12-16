package org.meepo.user;

import java.util.ArrayList;

import org.apache.log4j.Logger;

public abstract class SyncFactory {

	protected SyncFactory() {
		watcherThread.setDaemon(true);
		watcherThread.start();
	}

	protected abstract void sync();

	protected DoerThread createNewDoerThread() {
		DoerThread t = new DoerThread();
		t.setDaemon(true);
		return t;
	}

	protected void updateLastDoTime() {
		synchronized (lastDoTime) {
			lastDoTime = System.currentTimeMillis();
		}
	}

	protected long getLastDoTime() {
		synchronized (lastDoTime) {
			return lastDoTime;
		}
	}

	protected boolean isExpireDoTime() {
		Long difference = System.currentTimeMillis() - this.getLastDoTime();
		if (difference > EXPIRE_TIME_MILLIS || difference < 0L) {
			return true;
		} else {
			return false;
		}
	}

	protected Long lastDoTime = 0L;
	protected WatcherThread watcherThread = new WatcherThread();
	protected DoerThread doerThread;

	protected class WatcherThread extends Thread {
		public void run() {
			try {
				logger.info(String.format("Sync watcher thread starts in %s.",
						SyncFactory.this.getClass().getName()));
				// long syncCount = 0;
				while (run) {
					// Watcher thread could sleep longer, 30s for example.
					sleep(WATCHER_SLEEP_TIME);

					if (isExpireDoTime()) {
						if (doerThread != null) {
							doerThread.interrupt();
						}

						doerThread = createNewDoerThread();
						SyncFactory.this.updateLastDoTime();
						doerThread.start();
					}

					// logger.debug(String.format("Sync watcher thread is alive and has done %d times sync in %s.",
					// ++syncCount, this.getClass().getName()));
				}

				// Finalize work
				if (doerThread != null) {
					doerThread.interrupt();
				}
			} catch (InterruptedException e) {
				logger.debug(String.format(
						"Sync watcher thread stops unexpectedly in %s.", this
								.getClass().getName()));
				logger.error(e);
			}
			logger.debug(String.format("Sync watcher thread ends in %s.", this
					.getClass().getName()));
		}

		public void farewell() {
			run = false;
			this.interrupt();
		}

		private boolean run = true;
		private Logger logger = Logger.getLogger(WatcherThread.class);
		private static final long WATCHER_SLEEP_TIME = 1 * 60 * 1000; // a
																		// minute
	};

	protected class DoerThread extends Thread {
		public void run() {
			try {
				// logger.info(String.format("Sync doer thread starts in %s.",
				// this.getClass().getName()));
				// long syncCount = 0;
				while (true) {
					sleep(1000);
					sync();
					SyncFactory.this.updateLastDoTime();
				}
			} catch (InterruptedException e) {
				logger.debug(String.format(
						"Sync doer thread stops unexpectedly in %s.", this
								.getClass().getName()));
				logger.error("", e);
			}
			// logger.debug(String.format("Sync doer thread ends in %s.",
			// this.getClass().getName()));
		}

		private Logger logger = Logger.getLogger(WatcherThread.class);
	};

	public static void farewell() {
		for (WatcherThread wt : watcherThreads) {
			wt.farewell();
		}
	}

	private static ArrayList<WatcherThread> watcherThreads = new ArrayList<WatcherThread>();
	// private final static Long EXPIRE_TIME_MILLIS = 5* 1000L;
	private final static Long EXPIRE_TIME_MILLIS = 60 * 60 * 1000L;// 60 min
	protected static Logger logger = Logger.getLogger(SyncFactory.class);
}
