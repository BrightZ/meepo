package org.meepo.hyla.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.meepo.hyla.io.Writable;
import org.meepo.hyla.util.HashUtils;

public abstract class Storage implements Writable {
	protected String address;
	protected int port;
	protected long capacity = 0;
	protected long stableness = 0;
	protected long bandwidth = 0;
	protected List<String> paths = null;

	protected Storage() {
	}

	protected Storage(String address, int port, List<String> paths) {
		this.address = address;
		this.port = port;
		this.paths = new ArrayList<String>(paths);
	}

	public abstract String getURL();

	public abstract List<String> getPaths();

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeUTF(this.address);
		output.writeInt(this.port);
		output.writeLong(this.capacity);
		output.writeLong(this.stableness);
		output.writeLong(this.bandwidth);
		output.writeLong(this.paths.size());
		for (String i : this.paths) {
			output.writeUTF(i);
		}

	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.address = input.readUTF();
		this.port = input.readInt();
		this.capacity = input.readLong();
		this.stableness = input.readLong();
		this.bandwidth = input.readLong();

		this.paths = new ArrayList<String>();
		long paths_size = input.readLong();
		for (long i = 0; i < paths_size; i++) {
			paths.add(input.readUTF());
		}

	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Storage)) {
			return false;
		}
		Storage that = (Storage) obj;
		return (this.address.equals(that.address)) && (this.port == that.port);
	}

	@Override
	public int hashCode() {
		int hash = this.address.hashCode();
		hash = HashUtils.LARGE_PRIME * hash + this.port;
		return hash;
	}
}
