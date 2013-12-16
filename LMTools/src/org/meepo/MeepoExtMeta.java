package org.meepo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.common.ResponseCode;
import org.meepo.hyla.ExtendedMeta;

public class MeepoExtMeta extends ExtendedMeta {

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		output.writeInt(this.version);
		output.writeUTF(sha1);
		output.writeInt(replicaNumber);
		output.writeInt(permission);
		// output.writeUTF(this.custome);
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		this.version = input.readInt();
		this.sha1 = input.readUTF();
		this.replicaNumber = input.readInt();
		this.permission = input.readInt();
		// this.custome = input.readUTF();
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getReplicaNumber() {
		return replicaNumber;
	}

	public void setReplicaNumber(int replicaNumber) {
		this.replicaNumber = replicaNumber;
	}

	public int getPermission() {
		return permission;
	}

	public void setPermission(int permission) {
		this.permission = permission;
	}

	public String getSha1() {
		return sha1;
	}

	public void setSha1(String sha1) {
		this.sha1 = sha1;
	}

	private int version = 0;
	private int replicaNumber = 1;
	private int permission = ResponseCode.ACCESS_READ_ONLY;
	private String sha1 = "";
	// public String custome = "";
}
