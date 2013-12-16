package org.meepo.dba;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.meepo.config.Environment;
import org.meepo.user.Group;
import org.meepo.user.Group.Type;
import org.meepo.user.GroupFactory;
import org.meepo.user.UGRelation;
import org.meepo.user.User;
import org.meepo.user.UserFactory;

public class JoomlaDBClient {

	private JoomlaDBClient() {
		this.initialize();
	}

	private synchronized void initialize() {
		if (initialized)
			return;
		Properties props = new Properties();
		try {
			props.load(JoomlaDBClient.class
					.getResourceAsStream("JoomlaSql.properties"));
			// userAuthenticateSql = props.getProperty("user_authenticate_sql");
			getUserGroupSql = props.getProperty("get_user_group_sql");
			getPublicGroupSql = props.getProperty("get_public_group_sql");
			getUserGroupRelationSql = props
					.getProperty("get_user_group_relation_sql");
			getGroupSql = props.getProperty("getGroupSql");
			getUserSql = props.getProperty("getUserSql");
			getAllPublicSql = props.getProperty("get_all_public_sql");
			getAllGroupSql = props.getProperty("get_all_group_sql");

			String dbUrlString = String
					.format("jdbc:mysql://%s:%s/%s?"
							+ "user=%s&password=%s&useUnicode=true&characterEncoding=UTF-8",
							Environment.db_host, Environment.db_port,
							Environment.db_database, Environment.db_username,
							Environment.db_password);
			String driverClassName = "com.mysql.jdbc.Driver";
			String poolName = "Joomla";
			pool = new DBCP(driverClassName, dbUrlString, poolName);
		} catch (IOException e) {
			logger.error("", e);
		}

		initialized = true;
	}

	public DBCP getPool() {
		return this.pool;
	}

	public List<UGRelation> getUserJoinedGroups(User user, Group.Type type) {
		ArrayList<UGRelation> al = new ArrayList<UGRelation>();
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stat = null;

		try {
			conn = pool.getConnection();
			if (conn != null) {
				if (user.isRoot()) {
					String query = (type == Type.PUBLIC) ? getAllPublicSql
							: getAllGroupSql;
					stat = conn.prepareStatement(query);
					rs = stat.executeQuery();
					while (rs.next()) {
						String groupName = rs.getString("name");
						Group group = GroupFactory.getInstance().getGroup(
								groupName);
						UGRelation.Relation r = org.meepo.user.UGRelation.Relation.MEMBER;
						UGRelation ugr = new UGRelation(user, group, r);
						al.add(ugr);
					}
					// rs.close();
					// stat.close();
					// pool.closeConnection(conn);
				} else {
					String query = (type == Type.PUBLIC) ? getPublicGroupSql
							: getUserGroupSql;
					stat = conn.prepareStatement(query);
					stat.setString(1, user.getEmail());
					rs = stat.executeQuery();
					while (rs.next()) {
						String groupName = rs.getString("name");
						Group group = GroupFactory.getInstance().getGroup(
								groupName);
						UGRelation.Relation r = this.getUserGroupRelation(
								user.getEmail(), groupName);
						if (r != UGRelation.Relation.NONE) {
							UGRelation ugr = new UGRelation(user, group, r);
							al.add(ugr);
						}
					}
					// rs.close();
					// stat.close();
					// pool.closeConnection(conn);

				}
			}
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (stat != null)
					stat.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (conn != null)
					pool.closeConnection(conn);
			} catch (Exception e) {
				logger.error("", e);
			}
			;
		}
		return al;
	}

	public Group getGroup(String groupName) {
		Group retGroup = null;
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stat = null;

		try {
			conn = pool.getConnection();
			if (conn == null) {

			} else {
				String query = getGroupSql;
				stat = conn.prepareStatement(query);
				stat.setString(1, groupName);
				rs = stat.executeQuery();
				if (rs.next()) {
					int i = rs.getInt("type");
					Group.Type type = (i == GROUP_PUBLIC) ? Type.PUBLIC
							: Type.REGULAR;
					retGroup = new Group(groupName, Environment.getDomain(),
							type);
				} else {
					logger.debug("The group requested does not exist."
							+ groupName);
				}
				// rs.close();
				// stat.close();
				// pool.closeConnection(conn);
			}
			return retGroup;
		} catch (SQLException e) {
			// about finally :
			// http://www.ibm.com/developerworks/cn/java/j-lo-finally/index.html
			logger.error("", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (stat != null)
					stat.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (conn != null)
					pool.closeConnection(conn);
			} catch (Exception e) {
				logger.error("", e);
			}
			;
		}
	}

	public User getUser(String email) {
		User retUser = null;
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stat = null;

		try {
			conn = pool.getConnection();
			if (conn != null) {
				String query = getUserSql;
				stat = conn.prepareStatement(query);
				stat.setString(1, email);
				rs = stat.executeQuery();
				if (rs.next()) {
					String passwordMd5Salt = rs.getString("password");
					retUser = UserFactory.getInstance().genUser(email,
							passwordMd5Salt, Environment.getDomain());
				} else {
				}
				// rs.close();
				// stat.close();
				// pool.closeConnection(conn);
			}
			return retUser;
		} catch (SQLException e) {
			logger.error("", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (stat != null)
					stat.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (conn != null)
					pool.closeConnection(conn);
			} catch (Exception e) {
				logger.error("", e);
			}
			;

		}
	}

	public UGRelation.Relation getUserGroupRelation(String email,
			String groupName) {
		org.meepo.user.UGRelation.Relation relation = org.meepo.user.UGRelation.Relation.NONE;
		// Get a list of group based on user email.
		// Check cache first
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stat = null;

		try {
			conn = pool.getConnection();
			if (conn != null) {
				String query = getUserGroupRelationSql;
				stat = conn.prepareStatement(query);
				stat.setString(1, email);
				stat.setString(2, groupName);
				rs = stat.executeQuery();
				if (rs.next()) {
					int r = rs.getInt(1);
					if (r == JoomlaDBClient.USER_GROUP_ADMIN) {
						relation = org.meepo.user.UGRelation.Relation.ADMIN;
					} else if (r == JoomlaDBClient.USER_GROUP_MEMBER) {
						relation = org.meepo.user.UGRelation.Relation.MEMBER;
					}
				} else {
					relation = org.meepo.user.UGRelation.Relation.NONE;
				}
				// rs.close();
				// stat.close();
				// pool.closeConnection(conn);
			}
			return relation;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (stat != null)
					stat.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			;
			try {
				if (conn != null)
					pool.closeConnection(conn);
			} catch (Exception e) {
				logger.error("", e);
			}
			;

		}
	}

	public static JoomlaDBClient getInstance() {
		return instance;
	}

	/**
	 * for test
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// List<Group> groups;
		// groups = instance.getUserJoinedGroup("msmummy@gmail.com",
		// Group.Type.REGULAR);
		// for (Group c : groups) {
		// System.out.println(c.getName());
		// }
		// groups = instance.getUserJoinedGroup("msmummy@gmail.com",
		// Group.Type.PUBLIC);
		// for (Group c : groups) {
		// System.out.println(c.getName());
		// }
		// org.meepo.user.UGRelation.Relation r =
		// instance.getUserGroupRelation("msmummy@gmail.com", "wo");
		// System.out.println(r);

		// Test for procedures in mysql

		String dbUrlString = null;
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			if (dbUrlString == null) {
				dbUrlString = String
						.format("jdbc:mysql://%s:%s/%s?"
								+ "user=%s&password=%s&useUnicode=true&characterEncoding=UTF-8",
								"thu.meepo.org", "3306", "tsinghua", "root",
								"40750378");
			}
			conn = DriverManager.getConnection(dbUrlString);
			String queryString = "call getRelationBetweenUserAndGroup(?,?);";
			PreparedStatement stat = conn.prepareStatement(queryString);
			stat.setString(1, "chenkang@tsinghua.edu.cn");
			// stat.setString(2, "系统相关ISO@清华大学");
			stat.setString(2, "Tv@清华大学");
			// stat.setString(2, "test");
			ResultSet rs = stat.executeQuery();

			if (rs.next()) {
				System.out.println(rs.getInt(1));
			} else {
				System.out.println("No relationship");
			}

			rs.close();
			stat.close();
			conn.close();

		} catch (ClassNotFoundException e) {
			logger.error("", e);
		} catch (SQLException e) {
			logger.error("", e);
		}

	}

	private DBCP pool = null;

	private static final int USER_GROUP_ADMIN = 1;
	private static final int USER_GROUP_MEMBER = 0;

	private static final int GROUP_PUBLIC = 1;
	// private static final int GROUP_REGULAR = 2;

	private boolean initialized = false;

	private static JoomlaDBClient instance = new JoomlaDBClient();

	private static Logger logger = Logger.getLogger(JoomlaDBClient.class);

	// private static String userAuthenticateSql;
	private static String getUserGroupSql;
	private static String getPublicGroupSql;
	private static String getUserGroupRelationSql;
	private static String getGroupSql;
	private static String getUserSql;
	private static String getAllPublicSql;
	private static String getAllGroupSql;

}
