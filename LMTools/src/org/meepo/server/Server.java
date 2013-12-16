package org.meepo.server;

public class Server {
	private Server(String aHost, short aPort) {
		this.host = aHost;
		this.port = aPort;
	}

	protected Server(String aHost, short aPort, Integer aDomain) {
		this(aHost, aPort);
		this.domain = aDomain;
	}

	protected Server(String aHost, short aPort, Integer aDomain,
			Long userCount, Long groupCount) {
		this(aHost, aPort, aDomain);
		this.groupCount = groupCount;
		this.userCount = userCount;
	}

	public String getHostPlusPort() {
		if (host != null & port != null) {
			return host + ':' + port;
		} else {
			return null;
		}
	}

	public Integer getDomain() {
		return this.domain;
	}

	public void setDomain(Integer aDomain) {
		this.domain = aDomain;
	}

	public String getHost() {
		return host;
	}

	public Short getPort() {
		return this.port;
	}

	public Long getUserCount() {
		return this.userCount;
	}

	public Long getGroupCount() {
		return this.groupCount;
	}

	@Override
	public String toString() {
		return String.format(
				"Domain:%s \tHost:%s:%d\tUserCount:%s\tGroupCount:%s", domain,
				host, port, userCount, groupCount);
	}

	private String host;
	private Short port;
	private Integer domain = 1;
	private Long userCount = 0L;
	private Long groupCount = 0L;
}
