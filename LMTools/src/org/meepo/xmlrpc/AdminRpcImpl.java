package org.meepo.xmlrpc;

import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;
import org.meepo.common.ErrorTips;
import org.meepo.common.ResponseCode;
import org.meepo.dba.CassandraClient;
import org.meepo.fs.Chunk;
import org.meepo.server.Server;
import org.meepo.server.ServerFactory;
import org.meepo.user.Token;
import org.meepo.user.User;
import org.meepo.user.UserFactory;
import org.meepo.webserver.MeepoServlet;

public class AdminRpcImpl implements AdminRpcInterface {

	@Override
	public int setUserRoute(String email, String nodeAddr)
			throws XmlRpcException {
		logger.info(String.format("Set user route, user: %s master:%s ", email,
				nodeAddr));
		// Old_TODO
		return 0;
	}

	@Override
	public int setGroupRoute(String groupName, String nodeAddr)
			throws XmlRpcException {
		logger.info(String.format("Set group route, group: %s master:%s ",
				groupName, nodeAddr));

		// Forward this request to the master node of Meepo token database;
		// Old_TODO
		return 0;
	}

	@Override
	public String getUserRoute(String email) {
		User u = UserFactory.getInstance().getUser(email);
		Server s = ServerFactory.getInstance().getServer(u.getDomain());
		return s.toString();
	}

	@Override
	public int getHylaGroupMaster(String group) {
		// Old_TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int listHylaGroup() {
		// Old_TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String loginWithEmail(String email) throws XmlRpcException {
		logger.info(String.format("Request to login from admin. %s", email));
		User user = UserFactory.getInstance().getUser(email);
		if (user != null) {
			// Handle this request by itself
			// Skip user Authenticate
			Token token = user.getWebToken();
			logger.info(String.format("Logged in. Email: %s, Token: %s", email,
					token));
			MeepoAssist.getInstance().checkUserFirstShowUp(user);
			return token.getTokenString();
		} else {
			throw new XmlRpcException(ResponseCode.USER_NOT_EXISTS,
					ErrorTips.USER_NOT_EXISTS);
		}
	}

	@Override
	public Object[] getServerList() {
		return null;
	}

	@Override
	public Object[] getSuggestedLocation(String clientIP) {
		if (clientIP.isEmpty())
			clientIP = MeepoServlet.getClientIpAddress();

		return null;
	}

	@Override
	public int getUserCount(boolean localOnly) {
		return CassandraClient.getInstance().getUserCount(localOnly).intValue();
	}

	@Override
	public int getGroupCount(boolean localOnly) {
		return CassandraClient.getInstance().getGroupCount(localOnly)
				.intValue();
	}

	@Override
	public int deleteGroup(String name) {
		// Group group = GroupFactory.getInstance().getGroup(name);
		// Old_TODO
		return 0;
	}

	@Override
	public int deleteUser(String email) {
		// Old_TODO
		return 0;
	}

	@Override
	public int removeUserFromGroup(String email, String groupName) {
		return 0;
	}

	@Override
	public int renameGroup(String oldName, String newName) {
		// Old_TODO
		return 0;
	}

	/**
	 * 
	 * @param groupName
	 * @param type
	 * @return
	 */
	@Override
	public int changeGroupType(String groupName, int type) {
		// Old_TODO
		return 0;
	}

	/**
	 * 
	 * @param localOnly
	 * @return data count in GiB
	 * @throws XmlRpcException
	 */
	@Override
	public int getDataCount(boolean localOnly) {
		// Handle this locally
		long retLong[] = MeepoAssist.getInstance().getPathCapacity("/");
		Long l = (retLong[1] / (1024 * 1024 * 1024));
		return l.intValue();
	}

	@Override
	public String getChunkPath(String chunkID) {
		String retPath = "";
		Chunk c = CassandraClient.getInstance().getChunk(chunkID);
		if (c != null) {
			retPath = c.path;
		}
		return retPath;
	}

	@Override
	public long getTotalRequestCount() {
		return 0;
		// Old_TODO
	}

	private static Logger logger = Logger
			.getLogger(org.meepo.xmlrpc.AdminRpcImpl.class);
}
