package org.meepo.config;

public class Config {

	public static final String DOT = ".";

	public static final String HOME_DIR = "./";
	// public static final String HOME_DIR = "/Users/msm/workspace/meepo/";

	public static final String HYLA_DB_PATH = HOME_DIR + "./database/hyla-db/";

	public static final String PATH = HOME_DIR + "config.xml";
	// public static final String PATH =
	// "/Users/msm/workspace/meepo/config.xml";
	public static final String HOST_STRING = "host";
	public static final String PORT_STRING = "port";
	public static final String PATH_STRING = "path";
	public static final String DOMAIN_STRING = "domain";
	public static final String STINGY_STRING = "stingy";
	public static final String CROSS_STRING = "cross";
	public static final String FIX_PERMISSION = "fix_permission";

	public static final String CRON_STRING = "cron";
	public static final String EXPIRE_DAY = "expire_day";

	public static final String MEEPO_SERVER_STRING = "MeepoServer";
	public static final String MEEPO_SERVER_HOST_STRING = MEEPO_SERVER_STRING
			+ DOT + HOST_STRING;
	public static final String MEEPO_SERVER_PORT_STRING = MEEPO_SERVER_STRING
			+ DOT + PORT_STRING;
	public static final String MEEPO_SERVER_DOMAIN_STRING = MEEPO_SERVER_STRING
			+ DOT + DOMAIN_STRING;
	public static final String MEEPO_SERVER_STINGY_STRING = MEEPO_SERVER_STRING
			+ DOT + STINGY_STRING;
	public static final String MEEPO_SERVER_CROSS_STRING = MEEPO_SERVER_STRING
			+ DOT + CROSS_STRING;
	public static final String MEEPO_SERVER_FIX_PERMISSION = MEEPO_SERVER_STRING
			+ DOT + FIX_PERMISSION;

	public static final String STORAGE_SERVER_STRING = "StorageServer";
	public static final String STORAGE_HGSTORAGE_STRING = STORAGE_SERVER_STRING
			+ DOT + "HylaHGStorage";
	public static final String STORAGE_HOST_STRING = STORAGE_SERVER_STRING
			+ DOT + HOST_STRING;
	public static final String STORAGE_PORT_STRING = STORAGE_SERVER_STRING
			+ DOT + PORT_STRING;

	public static final String MYSQL_STRING = "MysqlServer";
	public static final String MYSQL_HOST_STRING = MYSQL_STRING + DOT
			+ HOST_STRING;
	public static final String MYSQL_PORT_STRING = MYSQL_STRING + DOT
			+ PORT_STRING;
	public static final String MYSQL_DATABASE_STRING = MYSQL_STRING + DOT
			+ "database";
	public static final String MYSQL_USERNAME_STRING = MYSQL_STRING + DOT
			+ "username";
	public static final String MYSQL_PASSWORD_STRING = MYSQL_STRING + DOT
			+ "password";

	public static final String CASSANDRA_STRING = "CassandraServer";
	public static final String CASSANDRA_HOST_STRING = CASSANDRA_STRING + DOT
			+ HOST_STRING;
	public static final String CASSANDRA_PORT_STRING = CASSANDRA_STRING + DOT
			+ PORT_STRING;
	public static final String CASSANDRA_KEYSPACE_STRING = CASSANDRA_STRING
			+ DOT + "keyspace";

	// TrashCleaner
	public static final String TRASH_CLEANER_STRING = "TrashCleaner";
	public static final String TRASH_CLEANER_CRON_STRING = TRASH_CLEANER_STRING
			+ DOT + CRON_STRING;
	public static final String TRASH_CLEANER_EXPIRE_STRING = TRASH_CLEANER_STRING
			+ DOT + EXPIRE_DAY;

	// Capacity Unit : GiB

	public static final String DEFAULT_CAPACITY_STRING = "DefaultCapacity";
	public static final String MYSPACE_STRING = "myspace";
	public static final String GROUP_STRING = "group";
	public static final String PUBLIC_STRING = "public";

	public static final String DEFAULT_CAPACITY_MYSPACE_STRING = DEFAULT_CAPACITY_STRING
			+ DOT + MYSPACE_STRING;
	public static final String DEFAULT_CAPACITY_GROUP_STRING = DEFAULT_CAPACITY_STRING
			+ DOT + GROUP_STRING;
	public static final String DEFAULT_CAPACITY_PUBLIC_STRING = DEFAULT_CAPACITY_STRING
			+ DOT + PUBLIC_STRING;

}
