package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LongWritable implements Writable {
	private long value;

	public LongWritable() {
		this.value = 0;
	}

	public LongWritable(long value) {
		this.value = value;
	}

	public long getValue() {
		return this.value;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeLong(this.value);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.value = input.readLong();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof LongWritable)) {
			return false;
		}
		LongWritable that = (LongWritable) obj;
		return this.value == that.value;
	}

	@Override
	public int hashCode() {
		return (int) this.value;
	}
}
