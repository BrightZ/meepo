package org.meepo.dba;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.meepo.config.Environment;
import org.meepo.fs.Chunk;
import org.meepo.server.Server;
import org.meepo.server.ServerFactory;
import org.meepo.user.Group;
import org.meepo.user.GroupFactory;
import org.meepo.user.Token;
import org.meepo.user.UGRelation;
import org.meepo.user.UGRelation.Relation;
import org.meepo.user.User;
import org.meepo.user.UserFactory;

/**
 * 
 * @author MS We use Apache Cassandra 0.8.6 to store information about I. Meepo
 *         node information II. Token for user to login and do operations.
 *         III.Route information, locate each user to one specific Meepo node.
 * 
 *         The data structure we apply in Cassandra are as follows. KEYSPACE
 *         (Like a database in relational database): meepo COLUMN_FAMILY (Like a
 *         table): node KEY (Like a primary key): addr - the node address.
 *         COLUMNS addr - the same, just to set a column. token KEY token - the
 *         128 char token COLUMNS email - the user email. keep_alive - the token
 *         last renew time, in milliseconds. domain - where this token is
 *         registered. //Old_TODO user KEY \<email\> username password_md5 - in md5
 *         domain
 * 
 *         server KEY \<domain\> host port user_count group_count
 * 
 *         group KEY \<name\> domain type
 * 
 *         userJoinedGroups KEY email groupName - relation
 * 
 *         groupJoinedUsers KEY groupName email - relation
 * 
 *         chunkIndex KEY chunkID path domain replica
 */

public class CassandraClient {

	// private ColumnFamilyTemplate<String, String>

	public boolean initialize() {
		LOGGER.info("Initializing cassandra client.");
		meepoCluster = HFactory.getOrCreateCluster(CLUSTER_NAME, String.format(
				"%s:%d", Environment.cassandraHost, Environment.cassandraPort));
		do {
			if (!initKeyspace()) {
				break;
			}

			if (!initTemplate()) {
				break;
			}

			return true;
		} while (false);

		return false;
	}

	private boolean initKeyspace() {
		try {
			KeyspaceDefinition keyspaceDef = meepoCluster
					.describeKeyspace(Environment.cassandraKeyspace);

			ColumnFamilyDefinition cfDef2 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace, TOKEN_FAMILY_NAME);
			ColumnFamilyDefinition cfDef4 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace, USER_FAMILY_NAME);
			// cfDef4.setColumnType(ColumnType.SUPER);
			ColumnFamilyDefinition cfDef5 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace, GROUP_FAMILY_NAME);
			// cfDef4.setColumnType(ColumnType.SUPER);
			ColumnFamilyDefinition cfDef6 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace, SERVER_FAMILY_NAME);
			ColumnFamilyDefinition cfDef7 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace,
							USER_JOINED_GROUPS_FAMILY_NAME);
			ColumnFamilyDefinition cfDef8 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace,
							GROUP_JOINED_USERS_FAMILY_NAME);
			ColumnFamilyDefinition cfDef9 = HFactory
					.createColumnFamilyDefinition(
							Environment.cassandraKeyspace,
							CHUNK_INDEX_FAMILY_NAME);

			// ColumnFamilyDefinition cfDef7 = HFactory.
			// createColumnFamilyDefinition(Environment.cassandraKeyspace,
			// RELATION_FAMILY_NAME);

			List<ColumnFamilyDefinition> cfDefList = Arrays.asList(cfDef2,
					cfDef4, cfDef5, cfDef6, cfDef7, cfDef8, cfDef9);

			if (keyspaceDef == null) {
				LOGGER.info("System first boot, "
						+ "try to create cassandra keyspace");

				keyspaceDef = HFactory.createKeyspaceDefinition(
						Environment.cassandraKeyspace,
						"org.apache.cassandra.locator.NetworkTopologyStrategy",
						// "org.apache.cassandra.locator.SimpleStrategy",
						1, cfDefList);
				meepoCluster.addKeyspace(keyspaceDef);
			} else {
				// Check for each ColumnFamily, create if not exists.
				for (ColumnFamilyDefinition cfDef : cfDefList) {
					try {
						meepoCluster.addColumnFamily(cfDef);
					} catch (HInvalidRequestException e) {
						// the column family already exists.
					}
				}
			}
			AllOneConsistencyLevelPolicy c = new AllOneConsistencyLevelPolicy();
			keyspace = HFactory.createKeyspace(Environment.cassandraKeyspace,
					meepoCluster, c);
		} catch (HectorException e) {
			LOGGER.error("Failed to connect to cassandra", e);
			return false;
		}
		return true;
	}

	private boolean initTemplate() {
		try {
			tokenTemplate = new ThriftColumnFamilyTemplate<String, String>(
					keyspace, TOKEN_FAMILY_NAME, StringSerializer.get(),
					StringSerializer.get());
			userTemplate = new ThriftColumnFamilyTemplate<String, String>(
					keyspace, USER_FAMILY_NAME, StringSerializer.get(),
					StringSerializer.get());
			groupTemplate = new ThriftColumnFamilyTemplate<String, String>(
					keyspace, GROUP_FAMILY_NAME, StringSerializer.get(),
					StringSerializer.get());
			serverTemplate = new ThriftColumnFamilyTemplate<Integer, String>(
					keyspace, SERVER_FAMILY_NAME, IntegerSerializer.get(),
					StringSerializer.get());
			chunkIndexTemplate = new ThriftColumnFamilyTemplate<String, String>(
					keyspace, CHUNK_INDEX_FAMILY_NAME, StringSerializer.get(),
					StringSerializer.get());
			userJoinedGroupsTemplate = new ThriftColumnFamilyTemplate<String, String>(
					keyspace, USER_JOINED_GROUPS_FAMILY_NAME,
					StringSerializer.get(), StringSerializer.get());
			groupJoinedUsersTemplate = new ThriftColumnFamilyTemplate<String, String>(
					keyspace, GROUP_JOINED_USERS_FAMILY_NAME,
					StringSerializer.get(), StringSerializer.get());

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	public Chunk getChunk(String chunkID) {
		Chunk retChunk = null;
		try {
			ColumnFamilyResult<String, String> res = chunkIndexTemplate
					.queryColumns(chunkID);
			if (res.hasResults()) {
				int replica = res.getInteger(REPLICA_STRING);
				String path = res.getString(PATH_STRING);
				retChunk = new Chunk(chunkID, replica, path);
			}
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return retChunk;
	}

	public boolean putChunk(Chunk chunk) {
		boolean ret = false;
		try {
			ColumnFamilyUpdater<String, String> updater = chunkIndexTemplate
					.createUpdater(chunk.chunkID);

			updater.setString(PATH_STRING, chunk.path);
			updater.setInteger(REPLICA_STRING, chunk.replicaCount);
			chunkIndexTemplate.update(updater);
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return ret;
	}

	public User getUser(String email) {
		User retUser = null;
		try {
			ColumnFamilyResult<String, String> res = userTemplate
					.queryColumns(email);
			if (res.hasResults()) {
				String passwordMd5Salt = res.getString(PASSWORD_MD5_STRING);
				Integer domain = res.getInteger(DOMAIN_STRING);
				retUser = UserFactory.getInstance().genUser(email,
						passwordMd5Salt, domain);
			}
		} catch (Exception e) {
			LOGGER.fatal(String.format("CEX:%s.", email), e);
		}
		return retUser;
	}

	public boolean putUser(User user) {
		boolean ret = false;
		try {
			ColumnFamilyUpdater<String, String> updater = userTemplate
					.createUpdater(user.getEmail());

			updater.setString(PASSWORD_MD5_STRING, user.getPasswordMd5());
			updater.setInteger(DOMAIN_STRING, user.getDomain());
			if (!userTemplate.isColumnsExist(user.getEmail())) {
				incrementUserCount();
			}
			userTemplate.update(updater);
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal(
					String.format("CEX:%s.\t%s.", user.getEmail(),
							user.getWebToken()), e);
		}
		return ret;
	}

	public Group getGroup(String groupName) {
		Group retGroup = null;
		try {
			ColumnFamilyResult<String, String> res = groupTemplate
					.queryColumns(groupName);
			if (res.hasResults()) {
				Integer domain = res.getInteger(DOMAIN_STRING);
				Group.Type type = Group.Type.values()[res
						.getInteger(GROUP_TYPE_STRING)];
				retGroup = new Group(groupName, domain, type);
			}
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return retGroup;
	}

	public boolean putGroup(Group group) {
		boolean ret = false;
		try {
			ColumnFamilyUpdater<String, String> updater = groupTemplate
					.createUpdater(group.getName());
			updater.setInteger(DOMAIN_STRING, group.getDomain());
			updater.setInteger(GROUP_TYPE_STRING, group.getType().ordinal());
			if (!groupTemplate.isColumnsExist(group.getName())) {
				incrementGroupCount();
			}
			groupTemplate.update(updater);
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return ret;
	}

	private synchronized void incrementUserCount() {
		try {
			ColumnFamilyResult<Integer, String> res = serverTemplate
					.queryColumns(Environment.getDomain());
			Long count = res.getLong(USER_COUNT_STRING);
			if (count == null)
				count = 0L;
			count++;
			ColumnFamilyUpdater<Integer, String> updater = serverTemplate
					.createUpdater(Environment.getDomain());
			updater.setLong(USER_COUNT_STRING, count);
			serverTemplate.update(updater);
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
	}

	public Long getGroupCount(boolean localOnly) {
		Long sum = -1L;
		try {
			if (localOnly) {
				return this.getGroupCount(Environment.getDomain());
			} else {
				Iterable<Integer> iterable = new KeyIterator<Integer>(keyspace,
						SERVER_FAMILY_NAME, IntegerSerializer.get());
				for (Integer domain : iterable) {
					sum += this.getGroupCount(domain);
				}
			}
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return sum;
	}

	private Long getGroupCount(Integer domain) {
		try {
			ColumnFamilyResult<Integer, String> res = serverTemplate
					.queryColumns(domain);
			Long count = res.getLong(GROUP_COUNT_STRING);
			if (count != null && count >= 0)
				return count;
			else
				return 0L;
		} catch (Exception e) {
			LOGGER.error("", e);
			return 0L;
		}
	}

	private synchronized void incrementGroupCount() {
		ColumnFamilyResult<Integer, String> res = serverTemplate
				.queryColumns(Environment.getDomain());
		Long count = res.getLong(GROUP_COUNT_STRING);
		if (count == null)
			count = 0L;
		count++;
		ColumnFamilyUpdater<Integer, String> updater = serverTemplate
				.createUpdater(Environment.getDomain());
		updater.setLong(GROUP_COUNT_STRING, count);
		serverTemplate.update(updater);
	}

	public Long getUserCount(boolean localOnly) {
		if (localOnly) {
			return this.getUserCount(Environment.getDomain());
		} else {
			Long sum = 0L;
			Iterable<Integer> iterable = new KeyIterator<Integer>(keyspace,
					SERVER_FAMILY_NAME, IntegerSerializer.get());
			for (Integer domain : iterable) {
				sum += this.getUserCount(domain);
			}
			return sum;
		}
	}

	private long getUserCount(Integer domain) {
		ColumnFamilyResult<Integer, String> res = serverTemplate
				.queryColumns(domain);
		Long count = res.getLong(USER_COUNT_STRING);
		if (count != null && count >= 0)
			return count;
		else
			return 0;
	}

	public List<UGRelation> getUserGroupRelations(User user, Group group) {

		if (user != null && group != null) {
			ColumnFamilyResult<String, String> res = userJoinedGroupsTemplate
					.queryColumns(user.getEmail());
			if (res.hasResults()) {
				Integer i = res.getInteger(group.getName());
				UGRelation r;
				if (i == null) {
					r = new UGRelation(user, group, Relation.NONE);
				} else {
					r = new UGRelation(user, group, Relation.values()[i]);
				}
				return Arrays.asList(r);
			} else {
				return Arrays.asList();
			}
		} else if (user != null && group == null) {
			ColumnFamilyResult<String, String> res = userJoinedGroupsTemplate
					.queryColumns(user.getEmail());
			if (res.hasResults()) {
				Collection<String> groupNames = res.getColumnNames();
				ArrayList<UGRelation> al = new ArrayList<UGRelation>();
				for (String name : groupNames) {
					Group newGroup = GroupFactory.getInstance().getGroup(name);
					Integer i = res.getInteger(name);
					UGRelation.Relation type = UGRelation.Relation.values()[i];
					UGRelation ugr = new UGRelation(user, newGroup, type);
					al.add(ugr);
				}
				return al;
			} else {
				return Arrays.asList();
			}
		} else if (user == null && group != null) {
			// Old_TODO
			return null;
		} else {
			return Arrays.asList();
		}

	}

	public Server getServer(Integer domain) {
		Server retServer = null;
		try {
			ColumnFamilyResult<Integer, String> result = serverTemplate
					.queryColumns(domain);
			if (result.hasResults()) {
				String host = result.getString(SERVER_HOST_STRING);
				Integer portInt = result.getInteger(SERVER_PORT_STRING);
				if (host == null || portInt == null) {
					return null;
				}
				Long groupCount = result.getLong(GROUP_COUNT_STRING);
				Long userCount = result.getLong(USER_COUNT_STRING);
				if (groupCount == null) {
					groupCount = 0L;
				}
				if (userCount == null) {
					userCount = 0L;
				}
				return ServerFactory.getInstance().genServer(host,
						portInt.shortValue(), domain, userCount, groupCount);
			}
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return retServer;

	}

	public boolean putServer(Server server) {
		boolean ret = false;
		try {
			ColumnFamilyUpdater<Integer, String> updater = serverTemplate
					.createUpdater(server.getDomain());
			updater.setString(SERVER_HOST_STRING, server.getHost());
			updater.setInteger(SERVER_PORT_STRING, server.getPort().intValue());
			serverTemplate.update(updater);
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return ret;
	}

	public boolean putUserGroupRelation(UGRelation ugr) {
		boolean ret = false;
		try {
			ColumnFamilyUpdater<String, String> userUpdater = userJoinedGroupsTemplate
					.createUpdater(ugr.getUser().getEmail());
			ColumnFamilyUpdater<String, String> groupUpdater = groupJoinedUsersTemplate
					.createUpdater(ugr.getGroup().getName());
			userUpdater.setInteger(ugr.getGroup().getName(), ugr.getRelation()
					.ordinal());
			groupUpdater.setInteger(ugr.getUser().getEmail(), ugr.getRelation()
					.ordinal());
			userJoinedGroupsTemplate.update(userUpdater);
			groupJoinedUsersTemplate.update(groupUpdater);
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return ret;
	}

	public boolean registerToken(Token token) {
		boolean ret = false;
		try {
			ColumnFamilyUpdater<String, String> updater = tokenTemplate
					.createUpdater(token.getTokenString());

			updater.setString(EMAIL_STRING, token.getEmail());
			updater.setLong(TOKEN_RENEW_TIME_STRING, token.getRenewTime());
			updater.setLong(TOKEN_GEN_TIME_STRING, token.getGenTime());
			updater.setInteger(DOMAIN_STRING, Environment.getDomain());
			tokenTemplate.update(updater);
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal(String.format("CEX:%s.", token.getTokenString()), e);
		}
		return ret;
	}

	public boolean renewToken(Token token) {
		try {
			ColumnFamilyUpdater<String, String> updater = tokenTemplate
					.createUpdater(token.getTokenString());
			updater.setString(EMAIL_STRING, token.getEmail());
			updater.setLong(TOKEN_RENEW_TIME_STRING, token.getRenewTime());
			updater.setLong(TOKEN_GEN_TIME_STRING, token.getGenTime());
			updater.setInteger(DOMAIN_STRING, token.getDomain());
			tokenTemplate.update(updater);
			return true;
		} catch (Exception e) {
			LOGGER.fatal(String.format("CEX:%s.", token.getTokenString()), e);
			return false;
		}
	}

	public boolean unregisterToken(Token token) {
		boolean ret = false;
		try {
			tokenTemplate.deleteRow(token.getTokenString());
			ret = true;
		} catch (Exception e) {
			LOGGER.fatal("", e);
		}
		return ret;
	}

	public Token getToken(String tokenString) {
		Token retToken = null;
		try {
			ColumnFamilyResult<String, String> res = tokenTemplate
					.queryColumns(tokenString);

			String email = res.getString(EMAIL_STRING);
			Long genTime = res.getLong(TOKEN_GEN_TIME_STRING);
			Long renewTime = res.getLong(TOKEN_RENEW_TIME_STRING);
			Long expireTime = res.getLong(TOKEN_EXPIRE_TIME_STRING);
			Integer domain = res.getInteger(DOMAIN_STRING);

			if (email != null) {
				genTime = (genTime == null) ? 0 : genTime;
				renewTime = (renewTime == null) ? 0 : renewTime;
				expireTime = (expireTime == null) ? 0 : expireTime;
				domain = (domain == null) ? Environment.getDomain() : domain;
				retToken = new Token(email, tokenString, genTime, renewTime,
						expireTime, domain);
			}
		} catch (Exception e) {
			LOGGER.fatal(String.format("CEX:%s.", tokenString), e);
		}
		return retToken;

	}

	public Iterable<String> getTokenIterable() {
		Iterable<String> iterable = new KeyIterator<String>(keyspace,
				TOKEN_FAMILY_NAME, StringSerializer.get(), 10000);
		return iterable;
	}

	public static CassandraClient getInstance() {
		return instance;
	}

	private static CassandraClient instance = new CassandraClient();

	private Cluster meepoCluster;

	private Keyspace keyspace;
	private ColumnFamilyTemplate<String, String> tokenTemplate;
	//
	private ColumnFamilyTemplate<String, String> userTemplate;
	private ColumnFamilyTemplate<String, String> groupTemplate;
	private ColumnFamilyTemplate<Integer, String> serverTemplate;

	private ColumnFamilyTemplate<String, String> userJoinedGroupsTemplate;
	private ColumnFamilyTemplate<String, String> groupJoinedUsersTemplate;
	private ColumnFamilyTemplate<String, String> chunkIndexTemplate;

	private static final String TOKEN_FAMILY_NAME = "token";
	private static final String USER_FAMILY_NAME = "user";
	private static final String GROUP_FAMILY_NAME = "group";
	private static final String SERVER_FAMILY_NAME = "server";
	private static final String GROUP_JOINED_USERS_FAMILY_NAME = "groupJoinedUsers";
	private static final String USER_JOINED_GROUPS_FAMILY_NAME = "userJoinedGroups";
	private static final String CHUNK_INDEX_FAMILY_NAME = "chenckIndex";

	private static final String CLUSTER_NAME = "Meepo Cluster";

	private static final String TOKEN_GEN_TIME_STRING = "gen_time";
	private static final String TOKEN_RENEW_TIME_STRING = "keep_alive";
	private static final String TOKEN_EXPIRE_TIME_STRING = "expire_time";
	private static final String DOMAIN_STRING = "domain";
	private static final String EMAIL_STRING = "email";
	private static final String PASSWORD_MD5_STRING = "password_md5";
	private static final String USER_COUNT_STRING = "user_count";
	private static final String GROUP_COUNT_STRING = "group_count";
	private static final String GROUP_TYPE_STRING = "group_type";
	private static final String SERVER_HOST_STRING = "host";
	private static final String SERVER_PORT_STRING = "port";

	private static final String REPLICA_STRING = "replica";
	private static final String PATH_STRING = "path";

	private static final Logger LOGGER = Logger
			.getLogger(org.meepo.dba.CassandraClient.class.getName());
}
