package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.hyla.util.HashUtils;

public class CompoundKey implements Writable {
	private Writable prefix;
	private String suffix;

	public CompoundKey(Writable prefix, String suffix) {
		this.prefix = prefix;
		this.suffix = suffix;
	}

	public Writable getPrefix() {
		return this.prefix;
	}

	public String getSuffix() {
		return this.suffix;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		this.prefix.writeTo(output);
		output.writeUTF(this.suffix);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.prefix.readFrom(input);
		this.suffix = input.readUTF();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof CompoundKey)) {
			return false;
		}
		CompoundKey that = (CompoundKey) obj;
		return this.prefix.equals(that.prefix)
				&& this.suffix.equals(that.suffix);
	}

	@Override
	public int hashCode() {
		int hash = this.prefix.hashCode();
		hash = HashUtils.LARGE_PRIME * hash + this.suffix.hashCode();
		return hash;
	}

	@Override
	public String toString() {
		return "CompoundKey {prefix:" + this.prefix + " suffix:" + this.suffix
				+ "}";
	}
}
