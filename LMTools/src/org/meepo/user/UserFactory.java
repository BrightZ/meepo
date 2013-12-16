package org.meepo.user;

import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;
import org.meepo.dba.CassandraClient;
import org.meepo.dba.JoomlaDBClient;

public class UserFactory extends SyncFactory {
	public UserFactory() {
		super();
	}

	public User getUser(String email) {
		User retUser = null;
		retUser = userMap.get(email);
		if (retUser != null) {
			return retUser;
		}

		retUser = CassandraClient.getInstance().getUser(email);
		if (retUser != null) {
			userMap.put(email, retUser);
			return retUser;
		}

		retUser = this.syncUser(email);
		return retUser;
	}

	// public User getUserFromToken(String token) throws XmlRpcException {
	// String email = TokenManager.getInstance().getUserEmailFromToken(token);
	// User user = this.getUser(email);
	// return user;
	// }

	// /**
	// * First check the Cassandra database, if not there, then check the
	// Joomla's database.
	// * @param email
	// * @param password
	// * @return
	// */
	// public User getUser(String email, String password) throws IOException{
	// //Password is wrong, with 2 possible reasons.
	// //1. password is wrong.
	// //2. password has changed .
	//
	// User retUser = null;
	//
	// try {
	// retUser = userMap.get(email);
	// if (retUser != null &&
	// retUser.getPasswordMd5().equals(CommonUtil.MD5(password))) {
	// return retUser;
	// }
	//
	// retUser = CassandraClient.getInstance().getUser(email);
	// if (retUser != null &&
	// retUser.getPasswordMd5().equals(CommonUtil.MD5(password))) {
	// userMap.put(email, retUser);
	// return retUser;
	// }
	//
	// retUser = JoomlaDBClient.getInstance().getUser(email, password);
	// if (retUser != null &&
	// retUser.getPasswordMd5().equals(CommonUtil.MD5(password))) {
	// userMap.put(email, retUser);
	// CassandraClient.getInstance().putUser(retUser);
	// return retUser;
	// }
	//
	// return null;
	// } catch (IOException e) {
	// throw e;
	// }
	// }

	public User genUser(String aEmail, String aPasswordMd5Salt, Integer aDomain) {
		User user = new User(aEmail, aPasswordMd5Salt, aDomain);
		return user;
	}

	private User syncUser(String email) {
		User retUser1 = CassandraClient.getInstance().getUser(email);
		User retUser2 = JoomlaDBClient.getInstance().getUser(email);

		if (retUser2 != null) {
			if (retUser1 == null
					|| !retUser2.getPasswordMd5().equalsIgnoreCase(
							retUser1.getPasswordMd5())) {
				CassandraClient.getInstance().putUser(retUser2);
			}
			userMap.put(email, retUser2);
			return retUser2;
		} else if (retUser1 != null) {
			userMap.put(email, retUser1);
			return retUser1;
		} else {
			userMap.remove(email);
			return null;
		}
	}

	protected void sync() {
		Set<String> set = userMap.keySet();
		String emails[] = new String[set.size()];
		set.toArray(emails);

		for (String email : emails) {
			if (email == "") {
				userMap.remove(email);
				logger.fatal("Email Maynot be Empty!");
			} else {
				syncUser(email);
			}
		}
	}

	private HashMap<String, User> userMap = new HashMap<String, User>();

	// Static part
	private static class UserFactoryHolder {
		private static UserFactory instance = new UserFactory();
	}

	public static UserFactory getInstance() {
		return UserFactoryHolder.instance;
	}

	protected static Logger logger = Logger.getLogger(UserFactory.class);
}
