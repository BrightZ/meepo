package org.meepo.hyla.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FTPStorage extends Storage {
	private String username;
	private String password;

	protected FTPStorage() {
	}

	public FTPStorage(String address, int port, String username, String password) {
		// Old_TODO add paths
		// super(address, port,null);
		super(address, port, new ArrayList<String>());
		this.username = username;
		this.password = password;
	}

	@Override
	public String getURL() {
		// Old_TODO Auto-generated method stub
		return null;
	}

	public List<String> getPaths() {
		// Old_TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		super.writeTo(output);
		output.writeUTF(this.username);
		output.writeUTF(this.password);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		super.readFrom(input);
		this.username = input.readUTF();
		this.password = input.readUTF();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof FTPStorage)) {
			return false;
		}

		FTPStorage that = (FTPStorage) obj;
		return super.equals(obj) && this.username.equals(that.username)
				&& this.password.equals(that.password);
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("FTPStorage {address:");
		strBuilder.append(this.address);
		strBuilder.append(" port:");
		strBuilder.append(this.port);
		strBuilder.append(" username:");
		strBuilder.append(this.username);
		strBuilder.append(" password:");
		strBuilder.append(this.password);
		strBuilder.append("}");
		return strBuilder.toString();
	}
}
