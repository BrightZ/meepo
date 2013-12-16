package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.hyla.util.HashUtils;

public class ObjectId implements Writable, Comparable<ObjectId> {
	protected short domain = 0;
	protected short reserved = 0;
	protected int generation = 0;
	protected long identity = 0;

	public ObjectId() {
	}

	public ObjectId(short domain, int generation, long identity) {
		this.domain = domain;
		this.generation = generation;
		this.identity = identity;
	}

	public String toNameString() {
		StringBuilder stringBuilder = new StringBuilder();

		String domainString = Integer.toHexString(this.domain);
		for (int i = 0; i < 2 - domainString.length(); i++) {
			stringBuilder.append("0");
		}
		stringBuilder.append(domainString);

		String reservedString = Integer.toHexString(this.reserved);
		for (int i = 0; i < 2 - reservedString.length(); i++) {
			stringBuilder.append("0");
		}
		stringBuilder.append(reservedString);

		String generationString = Integer.toHexString(this.generation);
		for (int i = 0; i < 4 - generationString.length(); i++) {
			stringBuilder.append("0");
		}
		stringBuilder.append(generationString);

		String identityString = Long.toHexString(this.identity);
		for (int i = 0; i < 8 - identityString.length(); i++) {
			stringBuilder.append("0");
		}
		stringBuilder.append(identityString);

		return stringBuilder.toString();
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeShort(this.domain);
		output.writeShort(this.reserved);
		output.writeInt(this.generation);
		output.writeLong(this.identity);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.domain = input.readShort();
		this.reserved = input.readShort();
		this.generation = input.readInt();
		this.identity = input.readLong();
	}

	@Override
	public int compareTo(ObjectId that) {
		if (this == that) {
			return 0;
		}

		if (this.domain > that.domain) {
			return 1;
		} else if (this.domain < that.domain) {
			return -1;
		}

		if (this.reserved > that.reserved) {
			return 1;
		} else if (this.reserved < that.reserved) {
			return -1;
		}

		if (this.generation > that.generation) {
			return 1;
		} else if (this.generation < that.generation) {
			return -1;
		}

		if (this.identity > that.identity) {
			return 1;
		} else if (this.identity < that.identity) {
			return -1;
		}

		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof ObjectId)) {
			return false;
		}

		ObjectId that = (ObjectId) obj;
		return ((this.domain == that.domain)
				&& (this.reserved == that.reserved)
				&& (this.generation == that.generation) && (this.identity == that.identity));
	}

	@Override
	public int hashCode() {
		int hash = this.domain;
		hash = HashUtils.LARGE_PRIME * hash + this.reserved;
		hash = HashUtils.LARGE_PRIME * hash + this.generation;
		hash = (int) (HashUtils.LARGE_PRIME * hash + this.identity);
		return hash;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("ObjectId {domain:");
		strBuilder.append(this.domain);
		strBuilder.append(" reserved:");
		strBuilder.append(this.reserved);
		strBuilder.append(" generation:");
		strBuilder.append(this.generation);
		strBuilder.append(" identity:");
		strBuilder.append(this.identity);
		strBuilder.append("}");
		return strBuilder.toString();
	}

	@Override
	public ObjectId clone() {
		ObjectId newObjId = new ObjectId();
		newObjId.domain = this.domain;
		newObjId.reserved = this.reserved;
		newObjId.generation = this.generation;
		newObjId.identity = this.identity;
		return newObjId;
	}
}
