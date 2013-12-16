package org.meepo.webserver;

//import org.apache.xmlrpc.webserver.ServletWebServer;

import org.apache.log4j.Logger;
import org.meepo.config.Environment;

public class MeepoServletServer {

	public static void main(String[] args) {
		// Start Cassandra service.
		// CassandraServiceStarter cs = new CassandraServiceStarter();
		// if (!cs.start()) {
		// return;
		// }

		// Load MeePo Environment
		if (!Environment.initialize()) {
			// Initialization failed.
			logger.error("Failed to load Meepo Environment. Exit...");
			return;
		}

		// Start Jetty service.
		JettyServiceStarter js = new JettyServiceStarter();
		if (!js.start()) {
			return;
		}
	}

	private static Logger logger = Logger.getLogger(MeepoServletServer.class);
}