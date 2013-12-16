package org.meepo.monitor;

import java.util.concurrent.atomic.AtomicLong;

import org.json.simple.JSONObject;
import org.meepo.config.Environment;
import org.meepo.jsender.JsonSender;

public class Statistic {

	public Statistic() {
		for (int i = 0; i < 3600; i++) {
			requestCounts[i] = new AtomicLong(0L);
			onlinePulseCount[i] = new AtomicLong(0L);
		}
		IndexIncreaseThread t = new IndexIncreaseThread();
		t.start();
	}

	public void increTokenCycleRound() {
		this.tokenCycleRound++;
	}

	public long getTokenCycleRound() {
		return this.tokenCycleRound;
	}

	public void increTrashCycleRound() {
		this.trashCycleRound++;
	}

	public long getTrashCycleRound() {
		return this.trashCycleRound;
	}

	public void increRequestCount() {
		totalRequestCount.incrementAndGet();
		requestCounts[index].incrementAndGet();
	}

	public void increPulseCount() {
		onlinePulseCount[index].incrementAndGet();
	}

	public void increTokenCollectCount() {
		this.tokenCollectCount.incrementAndGet();
	}

	public void increTrashCleanedByte(long size) {
		this.trashCleanedByte.addAndGet(size);
	}

	public void tokenGCLive() {
		this.tokenGCAliveTime = System.currentTimeMillis();
	}

	public long getTokenGCAliveTime() {
		return this.tokenGCAliveTime;
	}

	public void trashGCLive() {
		this.trashGCAliveTime = System.currentTimeMillis();
	}

	public long getTrashGCAliveTime() {
		return this.trashGCAliveTime;
	}

	public long getRecycledTokenCount() {
		return tokenCollectCount.get();
	}

	public long getTrashCleanedByte() {
		return trashCleanedByte.get();
	}

	public long getTotalRequestCount() {
		return totalRequestCount.get();
	}

	public void notifyRequestTime(long t) {
		// caos is ok here
		if (t > maxRequestTime) {
			maxRequestTime = t;
		}
	}

	public long getMaxRequestTime() {
		return this.maxRequestTime;
	}

	public long getMaxRequestCount() {
		return this.maxRequestCount;
	}

	public long getCurrentCount() {
		return this.currRequestCount;
	}

	public long getBootTime() {
		return this.bootTime;
	}

	public long getCurrentOnlineUserCount() {
		return this.curOnlineUserCount;
	}

	public long getMaxOnlineUserCount() {
		return this.maxOnlineUserCount;
	}

	public void updateWebOnlineCount() {

		String url = "http://" + Environment.db_host + "/?option=online_number";
		String str = JsonSender.get(url, null);
		JSONObject obj = JsonSender.str2JsonObj(str);
		curWebOnlineCount = Long.parseLong(obj.get("number").toString());
		lastUpdateWebCount = System.currentTimeMillis();
	}

	private class IndexIncreaseThread extends Thread {
		public IndexIncreaseThread() {
			super();
			this.setDaemon(true);
		}

		public void run() {
			while (true) {
				try {
					sleep(1000);
					int r = index;
					index = (r + 1) % 3600;
					currRequestCount = requestCounts[r].getAndSet(0L);
					if (currRequestCount > maxRequestCount) {
						maxRequestCount = currRequestCount;
					}

					int pre = (index - INTERVAL + 3600) % 3600;
					curOnlineUserCount = onlinePulseCount[pre].getAndSet(0L);
					for (int i = 0; i < INTERVAL - 1; i++) {
						pre = (pre + 1 + 3600) % 3600;
						curOnlineUserCount += onlinePulseCount[pre].get();
					}

					if (System.currentTimeMillis() - lastUpdateWebCount > UPDATEWEB_INTERVAL) {
						updateWebOnlineCount();
					}

					curOnlineUserCount += curWebOnlineCount;

					if (curOnlineUserCount > maxOnlineUserCount) {
						maxOnlineUserCount = curOnlineUserCount;
					}

				} catch (InterruptedException e) {
				}
			}
		}
	}

	private AtomicLong totalRequestCount = new AtomicLong(0L);
	private AtomicLong[] requestCounts = new AtomicLong[3600]; // last hour?
	private AtomicLong tokenCollectCount = new AtomicLong(0L);
	private AtomicLong trashCleanedByte = new AtomicLong(0L);
	// private AtomicLong maxRequestTime = new AtomicLong(0L);
	// private AtomicInteger currentIndex = new AtomicInteger(0);
	private int index = 0;
	private long maxRequestTime = 0;
	private long maxRequestCount = 0;
	private long currRequestCount = 0;
	private long bootTime = System.currentTimeMillis();
	private long tokenGCAliveTime = 0L;
	private long tokenCycleRound = 0L;

	private long trashGCAliveTime = 0L;
	private long trashCycleRound = 0L;

	private AtomicLong[] onlinePulseCount = new AtomicLong[3600];
	private long curOnlineUserCount = 0L;
	private long maxOnlineUserCount = 0L;
	private long curWebOnlineCount = 0L;

	private long lastUpdateWebCount = 0L;
	private static final long UPDATEWEB_INTERVAL = 20L * 60L * 1000L;// 20 min

	private static class StatisticHolder {
		private static Statistic instance = new Statistic();
	}

	public static Statistic getInstance() {
		return StatisticHolder.instance;
	}

	public static final int INTERVAL = 30;

	public static void main(String arg[]) {
		Thread threads[] = new Thread[1000];

		for (int i = 0; i < 1000; i++) {
			threads[i] = new Thread() {
				public void run() {
					for (int j = 0; j < 10000; j++) {
						getInstance().increRequestCount();
						getInstance().increPulseCount();
					}
				}
			};
		}

		for (int i = 0; i < 1000; i++) {
			threads[i].start();
		}

		try {
			for (int i = 0; i < 1000; i++) {
				threads[i].join();
			}
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long max = getInstance().getMaxRequestCount();
		long maxfuck = getInstance().getMaxOnlineUserCount();
		System.out.println(max);
		System.out.println(maxfuck);
	}

}
