package org.meepo.firewall;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.meepo.common.CommonUtil;
import org.meepo.scheduler.MJob;
import org.meepo.scheduler.MScheduler;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class CipherManager extends MJob {

	private CipherManager() {
		this.jobName = "CipherManager";
		this.groupName = "FireWall";
		this.triggerName = "T-CipherManager";
		this.cron = CIPHER_MANAGER_CRON_STRING;
	}

	public Cipher genCipher() {
		Cipher ret;
		Long id = 0L;
		if (currentCipher != null) {
			id = currentCipher.getCipherId() + 1L;
		}
		String cipherString = CommonUtil.randomString(cipherLen);
		long cipherGenTime = System.currentTimeMillis();
		ret = new Cipher(cipherString, cipherGenTime, id);
		this.currentCipher = ret;
		cipherMap.put(id, ret);
		return ret;
	}

	public Cipher getCurrentCipher() {
		return this.currentCipher;
	}

	// if cipher does not exist , return null
	public Cipher getCipher(long id) {
		return cipherMap.get(id);
	}

	public void updateCipher() {
		// remove older cipher
		Cipher cipher = genCipher();
		if (cipherMap.size() > cipherMapSize) {
			cipherMap.remove(cipher.getCipherId() - cipherMapSize);
		}
	}

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		this.updateCipher();
	}

	// this method must be called , for start cipher cycling .
	@Override
	public boolean scheduleMJob() {
		if (mscheduler == null) {
			LOGGER.error("MScheduler is not started");
			return false;
		}
		if (mscheduler.isStarted()) {
			return mscheduler.addJob(this);
		} else {
			LOGGER.error("MScheduler is not started");
			return false;
		}
	}

	public static CipherManager getInstance() {
		return instance;
	}

	private static CipherManager instance = new CipherManager();

	private static HashMap<Long, Cipher> cipherMap = new HashMap<Long, Cipher>();
	private Cipher currentCipher = null;
	public static final int cipherLen = 256;
	private static final int cipherMapSize = 10; // maintain a cipherMapSize
													// cipher.
	private static final String CIPHER_MANAGER_CRON_STRING = "0 2 * * * ?";// 2
																			// am
																			// everyday

	MScheduler mscheduler = MScheduler.getInstance();

	private final Logger LOGGER = Logger.getLogger(CipherManager.class);
}
