package org.meepo.hyla;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.meepo.hyla.io.Writable;

public class Meta implements Writable {
	private boolean isDir = false;
	private long size = 0L;
	private String name = null;
	private long createTime = 0L;
	private long changeTime = 0L;
	private long modifyTime = 0L;
	private long accessTime = 0L;
	private long snapshot = 0L;

	public Meta() {
	}

	public Meta(String name, boolean isDir, long time) {
		this.name = name;
		this.isDir = isDir;
		if (this.isDir) {
			this.size = 4096;
		}

		this.createTime = this.changeTime = this.modifyTime = this.accessTime = time;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isDirectory() {
		return this.isDir;
	}

	public boolean isFile() {
		return !this.isDir;
	}

	public long getSize() {
		return this.size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getCreateTime() {
		return this.createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public long getChangeTime() {
		return this.changeTime;
	}

	public void setChangeTime(long changeTime) {
		this.changeTime = changeTime;
	}

	public long getModifyTime() {
		return this.modifyTime;
	}

	public void setModifyTime(long modifyTime) {
		this.modifyTime = modifyTime;
	}

	public long getAccessTime() {
		return this.accessTime;
	}

	public void setAccessTime(long accessTime) {
		this.accessTime = accessTime;
	}

	public long getSnapshot() {
		return this.snapshot;
	}

	public void setSnapshot(long aSnapshot) {
		this.snapshot = aSnapshot;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeBoolean(this.isDir);
		output.writeLong(this.size);
		output.writeUTF(this.name);
		output.writeLong(this.createTime);
		output.writeLong(this.changeTime);
		output.writeLong(this.modifyTime);
		output.writeLong(this.accessTime);
		output.writeLong(this.snapshot);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.isDir = input.readBoolean();
		this.size = input.readLong();
		this.name = input.readUTF();
		this.createTime = input.readLong();
		this.changeTime = input.readLong();
		this.modifyTime = input.readLong();
		this.accessTime = input.readLong();
		try {
			this.snapshot = input.readLong();
		} catch (EOFException e) {
			// Just to be compatible with previous version
		}

	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Meta {isDir:");
		strBuilder.append(this.isDir);
		strBuilder.append(" size:");
		strBuilder.append(this.size);
		strBuilder.append(" name:");
		strBuilder.append(this.name);
		strBuilder.append(" createTime:");
		strBuilder.append(this.createTime);
		strBuilder.append(" changeTime:");
		strBuilder.append(this.changeTime);
		strBuilder.append(" modifyTime:");
		strBuilder.append(this.modifyTime);
		strBuilder.append(" accessTime:");
		strBuilder.append(this.accessTime);
		strBuilder.append("}");
		return strBuilder.toString();
	}

}
