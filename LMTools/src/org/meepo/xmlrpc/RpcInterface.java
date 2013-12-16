package org.meepo.xmlrpc;

import java.util.Map;

import org.apache.xmlrpc.XmlRpcException;

public interface RpcInterface {

	public String login(String email, String password) throws XmlRpcException;

	public int mkFile(String token, String path) throws XmlRpcException;

	public int mkDir(String token, String path) throws XmlRpcException;

	public int delete(String token, String path) throws XmlRpcException;

	@Deprecated
	public int rmFile(String token, String path) throws XmlRpcException;

	@Deprecated
	public int rmDir(String token, String path) throws XmlRpcException;

	public int rmDirPosix(String token, String path) throws XmlRpcException;

	public Object[] listDir(String token, String path) throws XmlRpcException;

	public Object[] listRoot(String token, String lang) throws XmlRpcException;

	public int move(String token, String oldPath, String newPath)
			throws XmlRpcException;

	public int forceMove(String token, String oldPath, String newPath)
			throws XmlRpcException;

	public int setAttr(String token, String path, String sizeInString,
			String ctimeInString, String mtimeInString, String atimeInString)
			throws XmlRpcException;

	public String getAttr(String token, String path) throws XmlRpcException;

	public int setAccessControl(String token, String path, int accessMask)
			throws XmlRpcException;

	public int getAccessControl(String token, String path)
			throws XmlRpcException;

	public String getRootAttr(String token) throws XmlRpcException;

	public Object[] readFileEncrypted(String token, String path)
			throws XmlRpcException;

	public Object[] writeFileEncrypted(String token, String path, int lockTime)
			throws XmlRpcException;

	public int writeConfirm(String token, String path, String sizeInString)
			throws XmlRpcException;

	public int writeConfirmWithSha1AndVersion(String token, String path,
			String sizeInString, String sha1, int version)
			throws XmlRpcException;

	public Object[] getSha1AndVersion(String token, String path)
			throws XmlRpcException;

	public Object[] listDirWithSha1AndVersion(String token, String path)
			throws XmlRpcException;

	public Object[] listDirWithSha1AndVersionAndChildDirs(String token,
			String path) throws XmlRpcException;

	public int lockUser(String token, int lockTime) throws XmlRpcException;

	public int unlockUser(String token) throws XmlRpcException;

	public int getLockTime(String token) throws XmlRpcException;

	public int logout(String token) throws XmlRpcException;

	public int getReplicaNumber(String token, String path)
			throws XmlRpcException;

	public int setReplicaNumber(String token, String path, int replicaNumber)
			throws XmlRpcException;

	public Object[] listAllReplicatedDirs(String token, String path)
			throws XmlRpcException;

	public Object[] listAllAccessControlDirs(String token, String path)
			throws XmlRpcException;

	public int getUserGroupRelation(String token, String groupName)
			throws XmlRpcException;

	// public int[] getSpace(String token) throws XmlRpcException;

	public String getServerTimeUTC();

	public Object[] writeFileEncryptedWithExpectedSize(String token,
			String path, int lockTime, String expectedSizeInLong)
			throws XmlRpcException;

	Object[] getCapacityInfo(String token, String path) throws XmlRpcException;

	String login(String email, String password, String machineName, int version)
			throws XmlRpcException;

	Object[] listDirMaps(String token, String path) throws XmlRpcException;

	Map<Object, Object> getAttrMap(String token, String path)
			throws XmlRpcException;

	Map<Object, Object> getCapacityMap(String token, String path)
			throws XmlRpcException;

	Long getServerTime();

	public int move(String token, String oldPath, String newPath, boolean force)
			throws XmlRpcException;

	public int restore(String token, String path) throws XmlRpcException;

	public Object[] downloadRequest(String token, String path)
			throws XmlRpcException;

	public Object[] getDataServerTokenCipher(long id) throws XmlRpcException;

	public Object[] getCurrentDataServerTokenCipher() throws XmlRpcException;

}
