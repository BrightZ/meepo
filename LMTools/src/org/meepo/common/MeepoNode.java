package org.meepo.common;

public class MeepoNode {

	public MeepoNode(int id, String address, int port) {
		this.id = id;
		this.address = address;
		this.port = port;

		tokenPrefix = CommonUtil.MD5(id + "");
	}

	public String getTokenPrefix() {
		return tokenPrefix;
	}

	public void setTokenPrefix(String tokenPrefix) {
		this.tokenPrefix = tokenPrefix;
	}

	// Basic information about a meepo node
	private int id;
	private String address;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	private int port;

	// the token prefix of a node
	private String tokenPrefix;
}
