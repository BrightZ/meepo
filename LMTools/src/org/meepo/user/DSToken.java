package org.meepo.user;

public class DSToken {

	public DSToken(String url, String email, String dpString,
			long dsTokenGenTime) {
		this.url = url;
		this.email = email;
		this.dpString = dpString;
		this.dsTokenGenTime = dsTokenGenTime;
	}

	public static final String Version = "V1.0";
	private String url;
	private String email;
	private String dpString;
	private long dsTokenGenTime;
}
