package org.meepo.hyla;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.hyla.io.Writable;

public class Statistic implements Writable {
	private long startTime = 0;
	private long endTime = 0;

	private long aggregateSize = 0;
	private long numberOfFiles = 0;
	private long numberOfDirectories = 0;

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setAggregateSize(long aggregateSize) {
		this.aggregateSize = aggregateSize;
	}

	public long getAggregateSize() {
		return aggregateSize;
	}

	public void setNumberOfFiles(long numberOfFiles) {
		this.numberOfFiles = numberOfFiles;
	}

	public long getNumberOfFiles() {
		return numberOfFiles;
	}

	public void setNumberOfDirectories(long numberOfDirectories) {
		this.numberOfDirectories = numberOfDirectories;
	}

	public long getNumberOfDirectories() {
		return numberOfDirectories;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeLong(this.startTime);
		output.writeLong(this.endTime);
		output.writeLong(this.aggregateSize);
		output.writeLong(this.numberOfFiles);
		output.writeLong(this.numberOfDirectories);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.startTime = input.readLong();
		this.endTime = input.readLong();
		this.aggregateSize = input.readLong();
		this.numberOfFiles = input.readLong();
		this.numberOfDirectories = input.readLong();
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Statis {startTime:");
		strBuilder.append(this.startTime);
		strBuilder.append(" endTime:");
		strBuilder.append(this.endTime);
		strBuilder.append(" aggregateSize:");
		strBuilder.append(this.aggregateSize);
		strBuilder.append(" numberOfFiles:");
		strBuilder.append(this.numberOfFiles);
		strBuilder.append(" numberOfDirectories:");
		strBuilder.append(this.numberOfDirectories);
		strBuilder.append("}");
		return strBuilder.toString();
	}
}
