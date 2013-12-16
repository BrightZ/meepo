package org.meepo.firewall;

public class Cipher {

	public Cipher(String cipherString, long cipherGenTime, Long id) {
		this.cipherString = cipherString;
		this.cipherGenTime = cipherGenTime;
		this.cipherId = id;
	}

	public String getCipherString() {
		return this.cipherString;
	}

	public long getCipherGenTime() {
		return this.cipherGenTime;
	}

	public Long getCipherId() {
		return this.cipherId;
	}

	private String cipherString = "";
	private Long cipherId;
	private long cipherGenTime;// this var is useless. only for debug

}
