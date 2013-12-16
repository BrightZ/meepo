package org.meepo.xmlrpc;

import org.apache.xmlrpc.XmlRpcException;

public interface AdminRpcInterface {
	public int setUserRoute(String email, String nodeAddr)
			throws XmlRpcException;

	public int setGroupRoute(String groupName, String nodeAddr)
			throws XmlRpcException;

	// public int setHylaGroupMaster(String group, String addr) throws
	// XmlRpcException;

	public int getHylaGroupMaster(String group);

	public int listHylaGroup();

	public String loginWithEmail(String email) throws XmlRpcException;

	public Object[] getServerList();

	public Object[] getSuggestedLocation(String clientIP);

	public int getUserCount(boolean localOnly);

	public int getGroupCount(boolean localOnly);

	public String getUserRoute(String email);

	public int getDataCount(boolean localOnly);

	int deleteGroup(String name);

	int deleteUser(String email);

	int removeUserFromGroup(String email, String groupName);

	int renameGroup(String oldName, String newName);

	int changeGroupType(String groupName, int type);

	public String getChunkPath(String chunkID);

	long getTotalRequestCount();
}
