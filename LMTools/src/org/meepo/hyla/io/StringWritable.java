package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StringWritable implements Writable {
	private String value;

	public StringWritable() {
		this.value = null;
	}

	public StringWritable(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeUTF(this.value);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.value = input.readUTF();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof StringWritable)) {
			return false;
		}
		StringWritable that = (StringWritable) obj;
		return this.value.equals(that.value);
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}
}
