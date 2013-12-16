package org.meepo.dba;

import java.io.File;

import org.apache.log4j.Logger;
import org.meepo.config.Config;

import com.sleepycat.je.DatabaseException;

public class BdbManager {
	public synchronized static boolean initialize() {
		// if (initialized) {
		// initCount++;
		// return true;
		// }

		logger.info("Initializing Meepo Berkeley Db Manager");

		try {

			File hylaDbEnvHome = new File(Config.HYLA_DB_PATH);
			if (!hylaDbEnvHome.exists()) {
				hylaDbEnvHome.mkdirs();
			}

			// File meepoDbEnvHome = new File(meepoDbLocation);
			// if(!meepoDbEnvHome.exists()) {
			// meepoDbEnvHome.mkdirs();
			// }

			// EnvironmentConfig envConfig = new EnvironmentConfig();
			// Durability durability = new
			// Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
			// Durability.SyncPolicy.NO_SYNC,
			// Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);
			// envConfig.setAllowCreate(true);
			// envConfig.setTransactional(true);
			// envConfig.setLockTimeout(1, TimeUnit.SECONDS);
			// envConfig.setDurability(durability);

			// Old_TODO For now we use time consistency policy, technically we
			// should use commit point consistency policy.
			// TimeConsistencyPolicy consistencyPolicy = new
			// TimeConsistencyPolicy(1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);

			// ReplicationConfig repConfig = new ReplicationConfig();
			// repConfig.setGroupName(MeepoEnvironment.routeGroupName);
			// repConfig.setNodeName(MeepoEnvironment.routeNodeName);
			// repConfig.setNodeHostPort(MeepoEnvironment.routeNodeAddress);
			// repConfig.setHelperHosts(MeepoEnvironment.routeHelperAddress);
			// repConfig.setReplicaAckTimeout(10, TimeUnit.SECONDS);
			// repConfig.setConsistencyPolicy(consistencyPolicy);
			// repConfig.setMaxClockDelta(10, TimeUnit.SECONDS);
			// meepoDbEnvironment = new ReplicatedEnvironment(meepoDbEnvHome,
			// repConfig, envConfig);
			// meepoDbEnvironment.setStateChangeListener(meepoDbListener);

			// DatabaseConfig dbConfig = new DatabaseConfig();
			// dbConfig.setAllowCreate(true);
			// dbConfig.setTransactional(true);
			// dbConfig.setSortedDuplicates(false);

			// tokenDb = meepoDbEnvironment.openDatabase(null, "meepoTokenDb",
			// dbConfig);
			// if (tokenDb == null)
			// {
			// logger.fatal("Failed to open token database.");
			// initialized = false;
			// return false;
			// }
			//
			// //Initialize the route database
			// routeDb = meepoDbEnvironment.openDatabase(null, "meepoRouteDb",
			// dbConfig);
			// if (routeDb == null) {
			// logger.fatal("Failed to open route database.");
			// initialized = false;
			// return false;
			// }
			//
			// //Initialize the node database
			// nodeDb = meepoDbEnvironment.openDatabase(null, "meepoNodeDb",
			// dbConfig);
			// if (nodeDb == null) {
			// logger.fatal("Failed to open node database.");
			// initialized = false;
			// return false;
			// }

		} catch (DatabaseException e) {
			logger.error(e.toString(), e);
			return false;
		}

		logger.info("initialized database manager");
		// initialized = true;
		// initCount++;
		return true;
	}

	// public synchronized static boolean close() {
	// initCount--;
	// if (initCount > 0) {
	// return true;
	// }
	//
	// logger.info("closing database manager");

	// if (tokenDb != null) {
	// tokenDb.close();
	// }
	// if (routeDb != null) {
	// routeDb.close();
	// }
	//
	// if (nodeDb != null) {
	// nodeDb.close();
	// }
	//
	// if (meepoDbEnvironment != null) {
	// meepoDbEnvironment.close();
	// }

	// logger.info("Meepo database successfully closed.");
	// initialized = false;
	// return true;
	// }

	// public static Environment getMeepoDbEnvironment() {
	// return meepoDbEnvironment;
	// }

	// public static Database getTokenDb() {
	// return tokenDb;
	// }

	// public static Database getRouteDb() {
	// return routeDb;
	// }

	// public static Database getNodeDb() {
	// return nodeDb;
	// }

	// public static Charset getCharset() {
	// return charset;
	// }

	private static Logger logger = Logger.getLogger(BdbManager.class);

	// private static boolean initialized = false;
	// private static int initCount = 0;

	// private static Environment tokenDbEnvironment;
	// private static ReplicatedEnvironment meepoDbEnvironment;
	// Database to store token. Could this be different on every Meepo node? For
	// now we assume so.
	// private static Database tokenDb;
	// private static Database routeDb;
	// private static Database nodeDb;

	// public static String tokenDbLocation =
	// System.getProperty("java.io.tmpdir");
	// public static String meepoDbLocation = "./database/meepo-db/";
	// public static String routeDbLocation = "./meepo-db/route-db/";

}
