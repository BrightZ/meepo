package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.hyla.util.HashUtils;
import org.meepo.hyla.util.ReflectionUtils;

public class PolymorphismWritable implements Writable {
	private String className = null;
	private Writable object = null;

	public PolymorphismWritable() {
	}

	public PolymorphismWritable(Writable object) {
		this.className = object.getClass().getName();
		this.object = object;
	}

	public String getClassName() {
		return this.className;
	}

	public Writable getObject() {
		return this.object;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeUTF(this.className);
		this.object.writeTo(output);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.className = input.readUTF();
		this.object = ReflectionUtils.newClassInstance(this.className);
		this.object.readFrom(input);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof PolymorphismWritable)) {
			return false;
		}

		PolymorphismWritable that = (PolymorphismWritable) obj;
		return (this.className.equals(that.className) && this.object
				.equals(that.object));
	}

	@Override
	public int hashCode() {
		int hash = this.className.hashCode();
		hash = HashUtils.LARGE_PRIME * hash + this.object.hashCode();
		return hash;
	}

	@Override
	public String toString() {
		return "CompoundData {prefix:" + this.className + " suffix:"
				+ this.object + "}";
	}
}
