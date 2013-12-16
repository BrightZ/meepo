package org.meepo.webserver;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.log4j.Logger;

/**
 * Example how to use an embedded cassandra service.
 * 
 * Tests connect to localhost:9160 when the embedded server is running.
 * 
 */
public class CassandraServiceStarter {

	public boolean start() {
		logger.info("Starting Cassandra service....");
		try {
			File file = new File("./cassandra.yaml");
			URL url = file.toURI().toURL();
			System.setProperty("cassandra.config", url.toString());
			EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
			cassandra.start();
			logger.info("Succeed starting Cassandra service.");
			return true;
		} catch (IOException e) {
			logger.error("Failed in starting Cassandra service");
			return false;
		}
	}

	private Logger logger = Logger.getLogger(CassandraServiceStarter.class);
}