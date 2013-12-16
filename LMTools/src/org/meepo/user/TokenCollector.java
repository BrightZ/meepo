package org.meepo.user;

import org.apache.log4j.Logger;
import org.meepo.dba.CassandraClient;
import org.meepo.monitor.Statistic;

public class TokenCollector extends Thread {

	public TokenCollector() {
		super();
		this.setDaemon(true);
	}

	@Override
	public void run() {
		while (true) {
			try {
				collect();
				Thread.sleep(TOKEN_GC_PERIOD);
			} catch (Exception e) {
				logger.fatal("Collector interupted.", e);
			}
		}
	}

	public void collect() throws Exception {
		Iterable<String> keys = CassandraClient.getInstance()
				.getTokenIterable();
		Statistic.getInstance().increTokenCycleRound();
		long count1 = 0L;
		long count2 = 0L;

		for (String k : keys) {
			count1++;
			Token token = CassandraClient.getInstance().getToken(k);
			if (token != null && token.isLocal() && token.isExpired()) {
				TokenFactory.getInstance().purgeToken(token);
				Statistic.getInstance().increTokenCollectCount();
				count2++;
			}
			Statistic.getInstance().tokenGCLive();
		}
		logger.info(String.format("Total token count: %d, purged token: %d",
				count1, count2));
	}

	private static final long TOKEN_GC_PERIOD = 1 * 3600 * 1000;
	private static Logger logger = Logger.getLogger(TokenCollector.class);
}
