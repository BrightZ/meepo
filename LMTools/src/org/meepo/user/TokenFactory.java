package org.meepo.user;

import java.util.Collection;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.meepo.dba.CassandraClient;

public class TokenFactory extends SyncFactory {

	public Token genToken(User user) {
		Token token = new Token(user);
		// Write to local hashmap
		tokenMap.put(token.getTokenString(), token);
		CassandraClient.getInstance().registerToken(token);
		logger.info("Register token:" + token);
		return token;
	}

	public Token getToken(String tokenString) {
		Token retToken = null;

		// Try to get from local hash map
		retToken = tokenMap.get(tokenString);

		// Try to get from cassandra database
		if (retToken == null) {
			retToken = CassandraClient.getInstance().getToken(tokenString);
			if (retToken != null) {
				tokenMap.put(tokenString, retToken);
			}
		}

		return retToken;
	}

	public boolean purgeToken(Token token) {
		logger.info(String.format("Recycle-Token %s %s",
				token.getTokenString(), token.getEmail()));

		boolean ret = false;
		// Remove from local hash map
		tokenMap.remove(token.getTokenString());

		// Try to remove from cassandra
		ret = CassandraClient.getInstance().unregisterToken(token);

		return ret;
	}

	@Override
	protected void sync() {

		Collection<Token> collec = tokenMap.values();
		Token tokens[] = new Token[collec.size()];
		tokens = collec.toArray(tokens);

		for (Token t : tokens) {
			if (t.isExpired()) {
				tokenMap.remove(t.getTokenString());
			} else if (t.postponeExpireTime()) {
				CassandraClient.getInstance().renewToken(t);
			}
		}

	}

	public static void main(String args[]) {
		// HashMap<String, String> testMap = new HashMap<String, String>();
		// for (int i = 0; i < 10; i++) {
		// testMap.put("key" + i, "value" + i);
		// }
		// // testMap.values().remove("value" + 5);
		// for (String value : testMap.values()) {
		// // if (value.va)
		// }
		// for (String key : testMap.keySet()) {
		// System.out.println(key);
		// }
	}

	private HashMap<String, Token> tokenMap = new HashMap<String, Token>();

	private static class TokenFactoryHolder {
		private static TokenFactory instance = new TokenFactory();
	}

	public static TokenFactory getInstance() {
		return TokenFactoryHolder.instance;
	}

	private static Logger logger = Logger.getLogger(TokenFactory.class);

}
