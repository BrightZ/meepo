package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IntegerWritable implements Writable {
	private int value;

	public IntegerWritable() {
		this.value = 0;
	}

	public IntegerWritable(int value) {
		this.value = value;
	}

	public int getValue() {
		return this.value;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeInt(this.value);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.value = input.readInt();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof IntegerWritable)) {
			return false;
		}
		IntegerWritable that = (IntegerWritable) obj;
		return this.value == that.value;
	}

	@Override
	public int hashCode() {
		return this.value;
	}
}
