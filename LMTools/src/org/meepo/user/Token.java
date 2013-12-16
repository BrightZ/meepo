package org.meepo.user;

import org.apache.log4j.Logger;
import org.meepo.common.CommonUtil;
import org.meepo.config.Environment;

public class Token {

	public Token(User user) {
		this.owner = user;
		this.email = this.owner.getEmail();
		this.tokenString = CommonUtil.randomString(128);
		this.genTime = System.currentTimeMillis();
		this.renewTime = this.genTime;
		this.expireTime = this.genTime + Token.LEASE;
		this.domain = Environment.getDomain();
	}

	public Token(String email, String tokenString, long genTime,
			long renewTime, long expireTime, int domain) {
		this.email = email;
		this.tokenString = tokenString;
		this.genTime = genTime;
		this.renewTime = renewTime;
		this.domain = domain;
	}

	public User getOwner() {
		if (this.owner == null) {
			this.owner = UserFactory.getInstance().getUser(email);
		}
		return this.owner;
	}

	public User getOwnerAndRenew() {
		User retUser = this.getOwner();
		this.renew();
		return retUser;
	}

	public void renew() {
		this.renewTime = System.currentTimeMillis();
	}

	public long getGenTime() {
		return this.genTime;
	}

	public long getRenewTime() {
		return this.renewTime;
	}

	public boolean isExpired() {
		long currTime = System.currentTimeMillis();
		boolean ret = false;

		if (this.expireTime > currTime) {
			return ret;
		}

		long result = currTime - renewTime;

		if (result >= LEASE) {
			ret = true;
			;
		} else if (result < -100000L) {
			logger.fatal(String.format("Token lease error. System Time:%d, %d",
					currTime, renewTime));
			ret = true;
		} else {
			ret = false;
		}
		return ret;
	}

	public boolean postponeExpireTime() {
		boolean ret = false;
		long minus = this.expireTime - this.renewTime;
		if (minus < (Token.LEASE / 10)) {
			this.expireTime = this.renewTime + Token.LEASE;
			ret = true;
		}
		return ret;
	}

	public boolean isLocal() {
		return (this.domain.equals(Environment.getDomain()));
	}

	public String getTokenString() {
		return this.tokenString;
	}

	public String getEmail() {
		if (this.email == null && this.owner != null) {
			this.email = this.owner.getEmail();
		}
		return this.email;
	}

	public Integer getDomain() {
		if (this.domain == null) {
			this.domain = Environment.getDomain();
		}
		return this.domain;
	}

	private String email;
	private String tokenString;
	private User owner;
	private long renewTime = 0L;
	private long genTime = 0L;
	private long expireTime = 0L;
	private Integer domain;

	public static final long LEASE = 6 * 3600 * 1000; // six hours

	public static final String publicTokenString = "00000000000000000000000000000000"
			+ "00000000000000000000000000000000"
			+ "00000000000000000000000000000000"
			+ "00000000000000000000000000000000";

	private static Logger logger = Logger.getLogger(Token.class);
}
