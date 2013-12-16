package org.meepo.hyla;

import java.io.IOException;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Stack;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.meepo.common.CommonUtil;
import org.meepo.config.Environment;
import org.meepo.dsa.DSClient;
import org.meepo.hyla.dist.DataSegment;
import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.storage.Storage;
import org.meepo.monitor.Statistic;
import org.meepo.scheduler.MJob;
import org.meepo.scheduler.MScheduler;
import org.meepo.xmlrpc.MeepoAssist;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class TrashCleaner extends MJob {

	private TrashCleaner() {
		this.jobName = "TrashCleaner";
		this.groupName = "Cleaner";
		this.triggerName = "T-TrashCleaner";
		this.cron = Environment.trash_cleaner_cron;
		this.expire_ms = Environment.trash_cleaner_expire_day;
	}

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

	private boolean mvData(String src, String dst) {
		boolean ret = false;
		process = null;
		try {
			String command[] = new String[] { "scp", src, dst };
			process = Runtime.getRuntime().exec(command);
			int retvalue = process.waitFor();
			if (retvalue != 0) {
				StringWriter writer = new StringWriter();
				IOUtils.copy(process.getErrorStream(), writer, "UTF-8");
				LOGGER.error(String.format("Scp error.\t%s\t%s\t%s", src, dst,
						writer.toString()));
			} else {
				ret = true;
				LOGGER.info(String.format("Scp success.%s.\t%s", src, dst));
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return ret;
	}

	private void doScan(FileObject root) {
		long diff;
		Meta meta;

		try {
			if (root.isDirectory()) {

				FileObject sons[] = root.list();
				for (FileObject son : sons) {
					meta = son.getMeta();
					diff = -1;
					if (meta != null) {
						diff = System.currentTimeMillis()
								- meta.getModifyTime();
					}

					if (son.isFile() && diff > this.expire_ms) {
						CleaningStack.push(son);
					} else if (son.isDirectory()) {
						doScan(son);
					}
				}

				meta = root.getMeta();
				if (sons.length == 0
						&& meta != null
						&& System.currentTimeMillis() - meta.getModifyTime() > this.expire_ms) {
					CleaningStack.push(root);
				}

			} else {
				meta = root.getMeta();

				if (meta != null
						&& System.currentTimeMillis() - meta.getModifyTime() > this.expire_ms) {
					CleaningStack.push(root);
				}
			}

		} catch (IllegalStateException e) {
			LOGGER.error("", e);
		} catch (IOException e) {
			LOGGER.error("", e);
		}

	}

	private void doClean() {
		FileObject fo;
		Distribution dist;
		DataSegment[] dsg;
		Storage storage;
		String fileName = "";
		long fileSize = 0L;

		while (!CleaningStack.isEmpty()) {
			fo = CleaningStack.pop();
			fileSize = 0L;
			try {
				fileName = fo.getRealPath();
				fileSize = fo.getMeta().getSize();
			} catch (Exception e) {
				LOGGER.error("", e);
				fileName = "";
			}

			try {
				if (fo.isDirectory() && fo.list().length == 0) {
					// empty dir -> delete
					fo.delete();
				} else if (fo.isFile()) {
					// get distribution
					dist = fo.getFileDistribution();
					// delete from hyla file system
					fo.delete();
					// delete distribution
					if (dist != null) {
						dsg = dist.getDataSegments();
						storage = assist.getHylaFileSystem().getStorage(
								dsg[0].getStorageId());
						if (storage != null) {
							String base = storage.getURL();
							String filePath = MeepoAssist.SLASH
									+ dsg[0].getPathOnStorage();
							String date = new SimpleDateFormat("yyyyMMdd")
									.format(new Date());
							String sha1 = CommonUtil.SHA1(filePath + date
									+ "123");

							// mv data to log server
							String srcHost = new URL(base).getHost();

							String srcString = "root@" + srcHost + ":"
									+ "/mnt/mfs" + filePath;

							// String dstString = "meepo@" + "log1.meepo.org" +
							// ":" +
							// "/mfschunks/sdb1/log/"+ filePath.replace('/',
							// '_');
							String dstString = "root@" + "ss1.thu.meepo.org"
									+ ":" + "/root/zorksylar/data/"
									+ filePath.replace('/', '_');

							if (!mvData(srcString, dstString)) {
								LOGGER.error(String.format(
										"TrashCleaner:%s\t%s\t%s", fileName,
										srcString, dstString));
								continue;
							}
							LOGGER.info(String.format(
									"TrashCleaner:%s\t%s\t%s", fileName,
									srcString, dstString));
							// delete data on ds
							int response = dsclient.deleteData(new Object[] {
									base, filePath, sha1 });
							switch (response) {
							case HttpURLConnection.HTTP_OK:
								LOGGER.info("DeleteData Success");
								Statistic.getInstance().increTrashCleanedByte(
										fileSize);
								break;
							case HttpURLConnection.HTTP_UNAUTHORIZED:
								LOGGER.error("Unauthorized");
								break;
							case HttpURLConnection.HTTP_FORBIDDEN:
								LOGGER.error("Permission Denyed");
								break;
							case HttpURLConnection.HTTP_NOT_FOUND:
								LOGGER.error("File Not Found");
								break;
							default:
								LOGGER.error("Unknown Error");
								break;
							}
						} else {
							LOGGER.error("Storage maynot be null");
						}
					} else {
						LOGGER.error("Distribution maynot be null");
					}
				}
			} catch (Exception e) {
				LOGGER.error("", e);
			}
		}
	}

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		Statistic.getInstance().trashGCLive();
		Statistic.getInstance().increTrashCycleRound();
		FileObject trashRoot = assist.getHylaFileSystem().openObject(
				MeepoAssist.TRASH_PREFIX);
		CleaningStack.clear();
		this.doScan(trashRoot);
		this.doClean();
	}

	public static TrashCleaner getInstance() {
		return instance;
	}

	private Stack<FileObject> CleaningStack = new Stack<FileObject>();
	private DSClient dsclient = DSClient.getInstance();
	private Process process;
	private static TrashCleaner instance = new TrashCleaner();

	MScheduler mscheduler = MScheduler.getInstance();
	MeepoAssist assist = MeepoAssist.getInstance();
	private long expire_ms;

	private final Logger LOGGER = Logger.getLogger(TrashCleaner.class);

}
