package org.meepo.xmlrpc;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;
import org.meepo.MeepoExtMeta;
import org.meepo.common.ErrorTips;
import org.meepo.common.ResponseCode;
import org.meepo.config.Environment;
import org.meepo.hyla.FileObject;
import org.meepo.hyla.FileSystem;
import org.meepo.hyla.OperationResponse;
import org.meepo.hyla.Statistic;
import org.meepo.user.Group;
import org.meepo.user.Token;
import org.meepo.user.User;

public class MeepoAssist {

	private MeepoAssist() {
	}

	public int handleHylaOS(OperationResponse ops) throws XmlRpcException {
		switch (ops) {
		case PARENT_NOT_EXISTS:
			throw new XmlRpcException(ResponseCode.PARENT_DIR_NOT_EXISTS,
					ErrorTips.PARENT_DIR_NOT_EXISTS);
		case OBJECT_NOT_EXISTS:
			throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
					ErrorTips.FILE_DIR_NOT_EXISTS);
		case OBJECT_ALREADY_EXISTS:
			throw new XmlRpcException(ResponseCode.FILE_DIR_ALREADY_EXISTS,
					ErrorTips.FILE_DIR_ALREADY_EXISTS);
		case UNEXPECTED_ERROR:
			throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
					ErrorTips.SYSTEM_ERROR);
		case SUCCESS:
			return ResponseCode.SUCCESS;
		default:
			throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
					ErrorTips.SYSTEM_ERROR);
		}
	}

	public boolean initGroupDirecotry(String groupName) {
		return false;
	}

	public String breakRealPathForCapacity(String realPath) {
		int firstSlashPos = realPath.indexOf(SLASH);
		int secondSlashPos = realPath.indexOf(SLASH, firstSlashPos + 1);
		int thirdSlashPos = realPath.indexOf(SLASH, secondSlashPos + 1);
		String retString;
		if (thirdSlashPos > 0) {
			retString = realPath.substring(firstSlashPos, thirdSlashPos);
		} else {
			retString = realPath;
		}
		return retString;
	}

	public FileSystem getHylaFileSystem() {
		return Environment.hylaFileSystem;
	}

	public static MeepoAssist getInstance() {
		return instance;
	}

	public void checkUserFirstShowUp(User u) throws XmlRpcException {
		// After Login success, require the partition the user belong to
		// If the user does not belong to any partition, then create home folder
		// for the user, and the user then belong to this partition.
		LOGGER.debug("User first time show up." + u.getEmail());
		FileObject f = getHylaFileSystem().openObject(
				MYSPACE_PREFIX_SUFFIX + u.getEmail() + SLASH);
		OperationResponse opr;
		try {
			opr = f.makeDirectory();
			if (opr == OperationResponse.SUCCESS) {
				MeepoExtMeta extMeta = new MeepoExtMeta();
				extMeta.setPermission(ResponseCode.ACCESS_WRITE_ALLOW);
				f.putExtendedMeta(extMeta);
			}
		} catch (Exception e) {
			throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
					"System unknown error when user first show up.");
		}
	}

	public void checkGroupFirstShowUp(Group group) throws XmlRpcException {
		String prefix = group.isPublic() ? PUBLIC_PREFIX : GROUP_PREFIX;
		String groupName = group.getName();
		FileObject groupDir = getHylaFileSystem().openObject(
				prefix + SLASH + groupName);
		try {
			if (!groupDir.exists()) {
				LOGGER.debug("Group first time show up." + groupName);
				groupDir.makeDirectory();
				MeepoExtMeta extMeta = new MeepoExtMeta();
				extMeta.setPermission(ResponseCode.ACCESS_READ_ONLY);
				groupDir.putExtendedMeta(extMeta);

				FileObject incomingDir = getHylaFileSystem().openObject(
						prefix + SLASH + groupName + SLASH + GROUP_UPLOAD);
				incomingDir.makeDirectory();
				extMeta = new MeepoExtMeta();
				extMeta.setPermission(ResponseCode.ACCESS_WRITE_ALLOW);
				incomingDir.putExtendedMeta(extMeta);

				// FileObject publicDir = getHylaFileSystem().openObject(prefix
				// + SLASH + groupName + SLASH + GROUP_PUBLIC);
				// publicDir.makeDirectory();
				// extMeta = new MeepoExtMeta();
				// extMeta.setPermission(ResponseCode.ACCESS_READ_ONLY);
				// publicDir.putExtendedMeta(extMeta);
			}
		} catch (Exception e) {
			LOGGER.error("", e);
			throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
		}
	}

	public synchronized boolean lockUser(Token token, long newExpireTime)
			throws XmlRpcException {
		synchronized (MeepoAssist.class) {
			String email = token.getOwnerAndRenew().getEmail();
			String lockToken = (String) userLockMap.get(email);
			// Already locked
			if (lockToken != null) {
				Long expireTime = (Long) userLockMap.get(lockToken);

				// not the same token && lock not expired
				if (!token.getTokenString().equals(lockToken)
						&& expireTime > System.currentTimeMillis() / 1000) {
					return false;
				} else
					userLockMap.remove(lockToken);
			}

			// Continue to lock
			userLockMap.put(email, token.getTokenString());
			userLockMap.put(token.getTokenString(), newExpireTime);
			return true;
		}
	}

	public boolean unlockUser(Token token) throws XmlRpcException {
		String email = token.getOwnerAndRenew().getEmail();
		synchronized (MeepoAssist.class) {
			String lockToken = (String) userLockMap.get(email);
			if (lockToken != null) {
				Long expireTime = (Long) userLockMap.get(lockToken);
				if (!token.getTokenString().equals(lockToken)
						&& expireTime > System.currentTimeMillis() / 1000) {
					return false;
				}
			}

			userLockMap.remove(email);
			userLockMap.remove(token.getTokenString());
			return true;
		}
	}

	public Long getLockExpireTime(String email) {
		synchronized (MeepoAssist.class) {
			String token = (String) userLockMap.get(email);
			if (token != null) {
				Long expireTime = (Long) userLockMap.get(token);
				return expireTime;
			}
			return null;
		}
	}

	/**
	 * 
	 * @param realPath
	 * @return capacity and used.
	 */
	public long[] getPathCapacity(String realPath) {
		long[] ret = new long[2];
		if (realPath.startsWith(MYSPACE_PREFIX)) {
			ret[0] = Environment.myspace_default_capacity;
			LOGGER.debug("MYSPACE_DEFAULT_CAPACITY: "
					+ Environment.myspace_default_capacity);
		} else if (realPath.startsWith(GROUP_PREFIX)) {
			ret[0] = Environment.group_default_capacity;
			LOGGER.debug("GROUP_DEFAULT_CAPACITY: "
					+ Environment.group_default_capacity);
		} else if (realPath.startsWith(PUBLIC_PREFIX)) {
			ret[0] = Environment.public_default_capacity;
			LOGGER.debug("PUBLIC_DEFAULT_CAPACITY: "
					+ Environment.public_default_capacity);
		}
		LOGGER.debug("ret[0]: " + ret[0]);

		String targetPath = this.breakRealPathForCapacity(realPath);
		FileObject userSpaceFile = this.getHylaFileSystem().openObject(
				targetPath);

		long used = 0;
		try {
			Statistic statistic = userSpaceFile.getDirectoryStatis();
			if (statistic == null) {
				// Statistic not ready
				used = 0;
			} else {
				used = statistic.getAggregateSize();
			}
		} catch (Exception e) {
			LOGGER.debug("Statistic error.", e);
			used = 0;
		}
		ret[1] = used;
		LOGGER.debug("Get capacity information:" + targetPath + "Capacity:"
				+ ret[0] + " Used:" + ret[1]);
		return ret;
	}

	private HashMap<String, Object> userLockMap = new HashMap<String, Object>();

	private static MeepoAssist instance = new MeepoAssist();
	private static Logger LOGGER = Logger.getLogger(MeepoAssist.class);

	public final static String SLASH = "/";
	public final static String DOT = ".";

	public final static String MYSPACE = "MySpace";
	public final static String GROUPS = "Groups";
	public final static String PUBLIC = "Public";
	public final static String TEMP = ".tmp";
	public final static String TRASH = ".Trash";

	public final static String MYSPACE_PREFIX = SLASH + MYSPACE;
	public final static String GROUP_PREFIX = SLASH + GROUPS;
	public final static String PUBLIC_PREFIX = SLASH + PUBLIC;
	public final static String TEMP_PREFIX = SLASH + TEMP;
	public final static String TRASH_PREFIX = SLASH + TRASH;

	public final static String MYSPACE_PREFIX_SUFFIX = SLASH + MYSPACE + SLASH;
	public final static String GROUP_PREFIX_SUFFIX = SLASH + GROUPS + SLASH;
	public final static String PUBLIC_PREFIX_SUFFIX = SLASH + PUBLIC + SLASH;
	public final static String TEMP_PREFIX_SUFFIX = SLASH + TEMP + SLASH;
	public final static String TRASH_PREFIX_SUFFIX = SLASH + TRASH + SLASH;

	public final static String GROUP_UPLOAD = "upload";
	public final static String GROUP_PUBLIC = "public";

	// public final static long MYSPACE_DEFAULT_CAPACITY = 20L * 1024L * 1024L *
	// 1024L; //20GiB
	// public final static long GROUP_DEFAULT_CAPACITY = 1L * 1024L * 1024L *
	// 1024L * 1024L; //1TiB
	// public final static long PUBLIC_DEFAULT_CAPACITY = 1L * 1024L * 1024L *
	// 1024L * 1024L * 1024L; //1PiB

	public final static String CAPACITY_TOTAL = "total";
	public final static String CAPACITY_USED = "used";
}
