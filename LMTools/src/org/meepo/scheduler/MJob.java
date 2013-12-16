package org.meepo.scheduler;

import org.quartz.Job;

public abstract class MJob implements Job {

	public String getJobName() {
		return this.jobName;
	}

	public String getGroupName() {
		return this.groupName;
	}

	public String getCron() {
		return this.cron;
	}

	public String getTriggerName() {
		return this.triggerName;
	}

	public abstract boolean scheduleMJob();

	protected String jobName;
	protected String groupName;
	protected String cron = "0 2 * * * ?";// 2 am everyday
	protected String triggerName;

}
