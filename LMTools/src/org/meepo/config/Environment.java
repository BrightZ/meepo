package org.meepo.config;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.meepo.HGStorage;
import org.meepo.MeepoExtMeta;
import org.meepo.common.ResponseCode;
import org.meepo.dba.BdbManager;
import org.meepo.dba.CassandraClient;
import org.meepo.fs.PermissionFixer;
import org.meepo.hyla.FileObject;
import org.meepo.hyla.FileSystem;
import org.meepo.hyla.OperationResponse;
import org.meepo.monitor.Monitor;
import org.meepo.scheduler.MScheduler;
import org.meepo.server.Server;
import org.meepo.server.ServerFactory;
import org.meepo.user.TokenCollector;
import org.meepo.xmlrpc.MeepoAssist;

public class Environment {

	public static boolean initialize() {
		if (isInitialized) {
			// The system has been initialized before, it's being called again
			// probably due to a web container reload.
			return true;
		}

		do {
			// Some initialization work.
			LOGGER.info("Initializing Meepo Environment");

			// The initialization order is important and could not change

			// Initialize Server Address.
			if (!initServerAddressConfig()) {
				LOGGER.error("Error happen when initilizing meepo route server address config.");
				break;
			}

			// Initialize Cassandra.
			if (!CassandraClient.getInstance().initialize()) {
				LOGGER.error("Error happen when initilizing cassandra.");
				break;
			}

			// Initialize Meepo Server
			initMeepoServer();

			// Initialize Berkeley DB. This should be after reading the
			// Meepo configure XML files.
			if (!BdbManager.initialize()) {
				LOGGER.error("Error happen when intializing meepo db.");
				break;
			}

			// Initialize meepo scheduler
			if (!initMeepoScheduler()) {
				LOGGER.error("Error happen when initializing meepo scheduler");
			} else {
				meepoScheduler.startSchedule();
			}

			// Although Hyla module could initialize automatically, here
			// we initialize it explicitly.
			if (!initHyla()) {
				LOGGER.error("Error happen when initializing hyla module.");
				break;
			}

			// Initialize several folders.
			if (!initFolders()) {
				LOGGER.error("Error happen when initializing folders.");
				break;
			}

			// Initialize handler for system exit
			initExitHandler();

			// Initialize char set, we use UTF-8, error should seldom happen
			// here.
			if (!initCharset()) {
				break;
			}

			if (Environment.fixPermission) {
				Thread t = new Thread() {
					public void run() {
						PermissionFixer.getInstance().fixAllGroupDirs();
					}
				};
				t.start();
			}

			// Start a token collector
			new TokenCollector().start();

			// Start monitor
			Monitor.getInstance().startInNewThread();

			LOGGER.info("Server initialized successfully.");

			isInitialized = true;
			return true;

		} while (false);

		// Initialize failed;
		isInitialized = false;
		return false;
	}

	public static boolean initHyla() {
		// A Default Hyla Environment Configure
		// hylaFileSystemConfig envConfig = new hylaFileSystemConfig();
		// envConfig.setAllowCreate(true);
		// hylaFileSystem = new hylaFileSystem(new
		// File(BdbManager.hylaDbLocation), envConfig);
		LOGGER.info("Initializing hyla.");
		boolean allowCreate = true;
		try {
			// Old_TODO domain should be integer value.
			hylaFileSystem = new FileSystem(new File(Config.HYLA_DB_PATH),
					allowCreate, meepoServer.getDomain().shortValue());
			LOGGER.debug("Just going to add storage." + storagehost + ":"
					+ storagePort);

			for (HGStorage s : storageList) {
				hylaFileSystem.addStorage(s);
			}

			return true;
		} catch (IOException e) {
			LOGGER.fatal("Init Hyla Failed.", e);
			return false;
		}
	}

	public static boolean initFolders() {
		LOGGER.debug("Prepare to initialize folders.");

		// Create the folder of MySpace, Groups, Public
		FileObject[] files = new FileObject[] {
				hylaFileSystem.openObject(MeepoAssist.MYSPACE_PREFIX_SUFFIX),
				hylaFileSystem.openObject(MeepoAssist.GROUP_PREFIX_SUFFIX),
				hylaFileSystem.openObject(MeepoAssist.PUBLIC_PREFIX_SUFFIX),
				hylaFileSystem.openObject(MeepoAssist.TEMP_PREFIX_SUFFIX) };

		// Set root permission and root time, we don't have to do it here now
		// cause now hyla does it automatically.
		// FileObject root = hylaFileSystem.openObject("/");
		// Meta meta = root.getMeta();
		// meta.setCustom(MeepoRes.ACCESS_READ_ONLY + "");
		// long currTimeInSeconds = System.currentTimeMillis() / 1000;
		// meta.setModifyTime(currTimeInSeconds);
		// meta.setCreateTime(currTimeInSeconds);
		// meta.setModifyTime(currTimeInSeconds);
		// root.setMeta(meta);

		for (int i = 0; i < files.length; i++) {
			FileObject file = files[i];
			try {
				OperationResponse opr = file.makeDirectory();
				if (opr == OperationResponse.SUCCESS) {
					MeepoExtMeta extMeta = new MeepoExtMeta();
					extMeta.setPermission(ResponseCode.ACCESS_READ_ONLY);
					file.putExtendedMeta(extMeta);
				}
			} catch (Exception e) {
				LOGGER.error("Fuck.", e);
				return false;
			}
		}
		return true;
	}

	public static boolean initServerAddressConfig() {
		LOGGER.info("Initializing server address config.");
		boolean ret = false;
		try {
			File configFile = new File(Config.PATH);
			LOGGER.debug("The config file path is:" + Config.PATH);

			if (!configFile.exists()) {
				LOGGER.error("Cannot find meepo server configuration file. Path:"
						+ configFile.getAbsolutePath());
			} else {

				XMLConfiguration config = new XMLConfiguration(configFile);

				host = config.getString(Config.MEEPO_SERVER_HOST_STRING);
				port = config.getShort(Config.MEEPO_SERVER_PORT_STRING);
				domain = config.getInt(Config.MEEPO_SERVER_DOMAIN_STRING);
				stingy = config.getBoolean(Config.MEEPO_SERVER_STINGY_STRING,
						false);
				cross = config.getBoolean(Config.MEEPO_SERVER_CROSS_STRING,
						false);
				fixPermission = config.getBoolean(
						Config.MEEPO_SERVER_FIX_PERMISSION, false);

				List<HierarchicalConfiguration> list = config
						.configurationsAt(Config.STORAGE_SERVER_STRING);
				for (HierarchicalConfiguration hc : list) {
					String host = hc.getString(Config.HOST_STRING);
					Integer port = hc.getInt(Config.PORT_STRING);
					List<String> paths = new ArrayList<String>(
							hc.getList(Config.PATH_STRING));
					HGStorage s = new HGStorage(host, port, paths);
					// LOGGER.error(s.toString());
					storageList.add(s);
				}

				db_host = config.getString(Config.MYSQL_HOST_STRING);
				db_port = config.getString(Config.MYSQL_PORT_STRING);
				db_database = config.getString(Config.MYSQL_DATABASE_STRING);
				db_username = config.getString(Config.MYSQL_USERNAME_STRING);
				db_password = config.getString(Config.MYSQL_PASSWORD_STRING);

				cassandraHost = config.getString(Config.CASSANDRA_HOST_STRING);
				cassandraPort = config.getShort(Config.CASSANDRA_PORT_STRING);
				cassandraKeyspace = config
						.getString(Config.CASSANDRA_KEYSPACE_STRING);

				myspace_default_capacity = config
						.getLong(Config.DEFAULT_CAPACITY_MYSPACE_STRING) * 1024L * 1024L * 1024L;
				group_default_capacity = config
						.getLong(Config.DEFAULT_CAPACITY_GROUP_STRING) * 1024L * 1024L * 1024L;
				public_default_capacity = config
						.getLong(Config.DEFAULT_CAPACITY_PUBLIC_STRING) * 1024L * 1024L * 1024L;

				trash_cleaner_cron = config
						.getString(Config.TRASH_CLEANER_CRON_STRING);
				trash_cleaner_expire_day = config
						.getLong(Config.TRASH_CLEANER_EXPIRE_STRING);

				ret = true;
			}

		} catch (ConfigurationException e) {
			LOGGER.fatal("Cannot load the configuration file.", e);
		} catch (IllegalStateException e) {
			LOGGER.fatal("Cannot init hyla file system.", e);
		}
		return ret;
	}

	private static void initMeepoServer() {
		LOGGER.info("Initializing meepo server.");
		meepoServer = ServerFactory.getInstance().getServer(domain);
		if (meepoServer == null) {
			// first time this server showed up.
			meepoServer = ServerFactory.getInstance().genServer(host, port,
					domain);
		}
	}

	private static boolean initMeepoScheduler() {
		boolean ret = false;
		LOGGER.info("Initializing meepo scheduler");
		meepoScheduler = MScheduler.getInstance();
		if (meepoScheduler != null) {
			ret = meepoScheduler.initScheduler();
		}
		return ret;
	}

	public static void initExitHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// System Finalize
				LOGGER.info("System is going to shut down.");
				Environment.isSystemStop = true;
				// BdbManager.close();
				if (hylaFileSystem != null) {
					hylaFileSystem.close();
				}
				if (meepoScheduler != null) {
					meepoScheduler.stopSchedule();
				}
			}
		});
	}

	public static void initZooKeeper() {

	}

	private static boolean initCharset() {
		try {
			charset = Charset.forName("UTF-8");
			return true;
		} catch (UnsupportedCharsetException e) {
			LOGGER.error("System does not support UTF-8 encoding.", e);
			return false;
		}
	}

	public static Charset getCharset() {
		return charset;
	}

	public static Integer getDomain() {
		return domain;
	}

	public static Short getMeepoPort() {
		return port;
	}

	public static FileSystem hylaFileSystem;

	private static Integer domain;
	private static String host;
	private static Short port;

	public static boolean isSystemStop = false;

	public static Server meepoServer;
	public static MScheduler meepoScheduler;

	public static String storagehost;
	public static int storagePort;
	public static ArrayList<HGStorage> storageList = new ArrayList<HGStorage>();

	public static String db_host;
	public static String db_port;
	public static String db_database;
	public static String db_username;
	public static String db_password;

	public static String cassandraHost;
	public static short cassandraPort;
	public static String cassandraKeyspace;

	public static boolean stingy = false;
	public static boolean cross = false;
	public static boolean fixPermission = false;

	public static long myspace_default_capacity;
	public static long group_default_capacity;
	public static long public_default_capacity;

	public static long trash_cleaner_expire_day;
	public static String trash_cleaner_cron;

	private static Charset charset;

	private static boolean isInitialized = false;

	public static final String MAGIC_STRING = "123";
	public static final String version = "20121020.3_ Debug Mode . \n Disabled :\na) TrashCleaner\nb) CipherManager";

	private static final Logger LOGGER = Logger.getLogger(MeepoAssist.class);

}
