package org.meepo.hyla.dist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.hyla.io.ObjectId;
import org.meepo.hyla.io.Writable;

public class DataSegment implements Writable {
	private ObjectId storageId;
	private String pathOnStorage;
	private long offset = 0;
	private long length = 0;
	private boolean isChecksum = false;

	protected DataSegment() {
	}

	public DataSegment(ObjectId storageId, String pathOnStorage) {
		this.storageId = storageId;
		this.pathOnStorage = pathOnStorage;
	}

	public DataSegment(ObjectId storageId, String pathOnStorage, long offset,
			long length) {
		this.storageId = storageId;
		this.pathOnStorage = pathOnStorage;
		this.offset = offset;
		this.length = length;
	}

	public ObjectId getStorageId() {
		return this.storageId;
	}

	public String getPathOnStorage() {
		return this.pathOnStorage;
	}

	public long getOffset() {
		return this.offset;
	}

	public void segOffset(long offset) {
		this.offset = offset;
	}

	public long getLength() {
		return this.length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public boolean isChecksum() {
		return this.isChecksum;
	}

	public void setIsChecksum(boolean isChecksum) {
		this.isChecksum = isChecksum;
	}

	public DataSegment(ObjectId storageId, String pathOnStorage, long offset,
			long length, boolean isChecksumSeg) {
		this(storageId, pathOnStorage, offset, length);
		this.isChecksum = isChecksumSeg;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		this.storageId.writeTo(output);
		output.writeUTF(this.pathOnStorage);
		output.writeLong(this.offset);
		output.writeLong(this.length);
		output.writeBoolean(this.isChecksum);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.storageId = new ObjectId();
		this.storageId.readFrom(input);
		this.pathOnStorage = input.readUTF();
		this.offset = input.readLong();
		this.length = input.readLong();
		this.isChecksum = input.readBoolean();
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("DataSegment {storageId:");
		strBuilder.append(this.storageId);
		strBuilder.append(" pathOnStorage:");
		strBuilder.append(this.pathOnStorage);
		strBuilder.append(" offset:");
		strBuilder.append(this.offset);
		strBuilder.append(" length:");
		strBuilder.append(this.length);
		strBuilder.append(" isChecksum:");
		strBuilder.append(this.isChecksum);
		strBuilder.append("}");
		return strBuilder.toString();
	}
}
