package org.meepo;

import java.util.List;

import org.meepo.hyla.storage.Storage;

public class HGStorage extends Storage {

	public HGStorage() {
		super();
	}

	public HGStorage(String address, int port, List<String> paths) {
		super(address, port, paths);
	}

	@Override
	public String getURL() {
		return "http://" + address + ":" + port + "/fcgi-bin/fs";
	}

	@Override
	public String toString() {
		return super.toString() + "\taddr:" + address + "\tport:" + port;

	}

	public List<String> getPaths() {
		return this.paths;
	}

	@Override
	public boolean equals(Object otherObject) {
		if (this == otherObject)
			return true;

		if (otherObject == null)
			return false;

		if (getClass() != otherObject.getClass())
			return false;
		HGStorage otherStorage = (HGStorage) otherObject;
		return address.equals(otherStorage.address)
				&& (port == otherStorage.port);
	}

}
