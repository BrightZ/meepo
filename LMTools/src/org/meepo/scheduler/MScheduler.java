package org.meepo.scheduler;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import org.apache.log4j.Logger;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerMetaData;
import org.quartz.impl.StdSchedulerFactory;

public class MScheduler {

	public boolean initScheduler() {
		boolean ret = false;
		this.schedulerFactory = new StdSchedulerFactory();
		try {
			this.scheduler = schedulerFactory.getScheduler();
			ret = true;
		} catch (SchedulerException e) {
			logger.fatal("initScheduler failed", e);
		}
		return ret;
	}

	// Add Job to scheduler ,and start the job
	public boolean addJob(MJob mj) {

		boolean ret = false;
		job = newJob(mj.getClass()).withIdentity(mj.getJobName(),
				mj.getGroupName()).build();

		trigger = newTrigger()
				.withIdentity(mj.getTriggerName(), mj.getGroupName())
				.withSchedule(
						cronSchedule(mj.getCron())
								.withMisfireHandlingInstructionIgnoreMisfires())// Ignore
																				// MisFire
				.build();
		try {
			scheduler.scheduleJob(job, trigger);
			ret = true;
		} catch (SchedulerException e) {
			logger.fatal(
					String.format("addJob failed.jobName:%s. groupName:%s",
							mj.getJobName(), mj.getGroupName()), e);
		}
		return ret;
	}

	public Object[] getStat() {
		Object[] stat = new Object[3];
		try {
			schedulerMetaData = scheduler.getMetaData();
			stat[0] = schedulerMetaData.getRunningSince(); // Date
			stat[1] = schedulerMetaData.getNumberOfJobsExecuted();// int
			stat[2] = schedulerMetaData.getSummary();// String
		} catch (SchedulerException e) {
			logger.fatal("", e);
		}
		return stat;
	}

	public boolean isShutdown() {
		boolean ret = true;
		try {
			ret = this.scheduler.getMetaData().isShutdown();
		} catch (SchedulerException e) {
			logger.error("", e);
		}
		return ret;
	}

	public boolean isStarted() {
		boolean ret = false;
		try {
			ret = this.scheduler.getMetaData().isStarted();
		} catch (SchedulerException e) {
			logger.error("", e);
		}
		return ret;
	}

	public boolean startSchedule() {
		boolean ret = false;
		try {
			this.scheduler.start();
			ret = true;
		} catch (SchedulerException e) {
			logger.fatal("startSchedule failed", e);
		}

		return ret;
	}

	public boolean stopSchedule() {
		boolean ret = false;
		try {
			this.scheduler.shutdown(false);
			ret = true;
		} catch (SchedulerException e) {
			logger.fatal("stopScheduler failed", e);
		}
		return ret;
	}

	public static MScheduler getInstance() {
		return instance;
	}

	private static MScheduler instance = new MScheduler();

	private SchedulerFactory schedulerFactory;
	private Scheduler scheduler;
	private SchedulerMetaData schedulerMetaData;

	private JobDetail job;
	private CronTrigger trigger;

	private static Logger logger = Logger.getLogger(MScheduler.class);
}
