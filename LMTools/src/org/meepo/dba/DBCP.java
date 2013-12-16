package org.meepo.dba;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

public class DBCP {

	public DBCP(String driverClassName, String connectURI, String poolName) {

		try {
			// load a particular database driver.
			Class.forName(driverClassName);

			// load apache pooling driver.
			Class.forName("org.apache.commons.dbcp.PoolingDriver");

			this.name = poolName;

			int maxActive = 300;
			byte whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
			long maxWait = 1 * 60 * 1000L;// 1 min
			int maxIdle = 50;
			int minIdle = 10;
			boolean testOnBorrow = false;//
			boolean testOnReturn = true;//
			boolean testWhileIdle = true;

			long timeBetweenEvictionRunsMillis = 30 * 60 * 1000L;// 30 min
			int numTestsPerEvictionRun = 300;// default
			long minEvictableIdleTimeMillis = 30 * 60 * 1000L; // default

			this.connectionPool = new GenericObjectPool(null, maxActive,
					whenExhaustedAction, maxWait, maxIdle, minIdle,
					testOnBorrow, testOnReturn, timeBetweenEvictionRunsMillis,
					numTestsPerEvictionRun, minEvictableIdleTimeMillis,
					testWhileIdle);

			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
					connectURI, null);

			PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
					connectionFactory, connectionPool, null, null, false, true);
			poolableConnectionFactory.getPool();

			PoolingDriver driver = (PoolingDriver) DriverManager
					.getDriver("jdbc:apache:commons:dbcp:");
			driver.registerPool(name, connectionPool);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public int getActiveCount() {
		return this.connectionPool.getNumActive();
	}

	public long getOpenedCount() {
		return this.openedConnectionCount.get();
	}

	public long getClosedCount() {
		return this.closedConnectionCount.get();
	}

	public Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection("jdbc:apache:commons:dbcp:"
					+ this.name);
		} catch (SQLException e) {
			// Old_TODO
			e.printStackTrace();
			logger.error("Mysql Error Marked! ", e);
		}
		openedConnectionCount.incrementAndGet();
		return conn;
	}

	public void closeConnection(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Mysql Error Marked! ", e);
		}
		closedConnectionCount.incrementAndGet();
	}

	private ObjectPool connectionPool;
	private String name;
	private AtomicLong openedConnectionCount = new AtomicLong(0L);
	private AtomicLong closedConnectionCount = new AtomicLong(0L);

	private static Logger logger = Logger.getLogger(DBCP.class);
}
