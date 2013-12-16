package org.meepo.server;

import java.util.HashMap;

import org.meepo.dba.CassandraClient;
import org.meepo.user.SyncFactory;

public class ServerFactory extends SyncFactory {

	public Server getServer(Integer domain) {
		Server retServer;

		retServer = serverMap.get(domain);
		if (retServer != null) {
			return retServer;
		}

		retServer = CassandraClient.getInstance().getServer(domain);
		if (retServer != null) {
			serverMap.put(domain, retServer);
			return retServer;
		}

		return null;
	}

	/**
	 * Used by {@link CassandraClient #getServer(Integer)}
	 * 
	 * @param aHost
	 * @param aPort
	 * @param aDomain
	 * @param userCount
	 * @param groupCount
	 * @return
	 */
	public Server genServer(String aHost, short aPort, Integer aDomain,
			Long userCount, Long groupCount) {
		Server server = new Server(aHost, aPort, aDomain, userCount, groupCount);
		serverMap.put(aDomain, server);
		return server;
	}

	/**
	 * Used by {@link org.meepo.config.Environment #initServerAddressConfig()}
	 * 
	 * @param aHost
	 * @param aPort
	 * @param aDomain
	 * @return
	 */
	public Server genServer(String aHost, short aPort, Integer aDomain) {
		Server server = new Server(aHost, aPort, aDomain);
		CassandraClient.getInstance().putServer(server);
		return server;
	}

	private HashMap<Integer, Server> serverMap = new HashMap<Integer, Server>();

	public static ServerFactory getInstance() {
		return instance;
	}

	private static ServerFactory instance = new ServerFactory();

	@Override
	protected void sync() {
		// Old_TODO Auto-generated method stub

	}

}
