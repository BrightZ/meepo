package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BooleanWritable implements Writable {
	private boolean value;

	public BooleanWritable() {
		this.value = false;
	}

	public BooleanWritable(boolean value) {
		this.value = value;
	}

	public boolean getValue() {
		return this.value;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeBoolean(this.value);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.value = input.readBoolean();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof BooleanWritable)) {
			return false;
		}
		BooleanWritable that = (BooleanWritable) obj;
		return this.value == that.value;
	}

	@Override
	public int hashCode() {
		if (this.value) {
			return 1;
		} else {
			return 0;
		}
	}
}
