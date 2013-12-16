package org.meepo.xmlrpc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;
import org.meepo.MeepoExtMeta;
import org.meepo.common.CommonUtil;
import org.meepo.common.ErrorTips;
import org.meepo.common.ResponseCode;
import org.meepo.firewall.Cipher;
import org.meepo.firewall.CipherManager;
import org.meepo.fs.AttributeInfo;
import org.meepo.fs.FileDistInfo;
import org.meepo.fs.Path;
import org.meepo.fs.PermissionFixer;
import org.meepo.fs.StorageEnum;
import org.meepo.hyla.FileObject;
import org.meepo.hyla.Meta;
import org.meepo.hyla.dist.DataSegment;
import org.meepo.hyla.dist.DefaultDistributionPolicy;
import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.storage.Storage;
import org.meepo.monitor.Statistic;
import org.meepo.user.Group;
import org.meepo.user.Group.Type;
import org.meepo.user.GroupFactory;
import org.meepo.user.Token;
import org.meepo.user.TokenFactory;
import org.meepo.user.UGRelation;
import org.meepo.user.User;
import org.meepo.user.UserFactory;
import org.meepo.webserver.MeepoServlet;

/**
 * 
 * @author msmummy, frog
 * 
 */
public class RpcImpl implements RpcInterface {

	/**
	 * For user to login. If successful, each user get a identical
	 * 128-characters long token, used for authentication in all other
	 * operations.
	 * 
	 * @param email
	 *            User's email address, as the identifier.
	 * @param password
	 *            For now, we use plain text.
	 * @param machineName
	 *            The name of the machine from which the client try to login.
	 *            Give empty string if you don't know the machine name.
	 * @param version
	 *            The version number of the client, a client should pass this
	 *            because someday the client version could be too old that it's
	 *            no longer supported, and a mature client should be able to
	 *            handle that situation. It's suggested to use date as version
	 *            number, such as "20110923". Set 0 if you don't know.
	 * @return A 128-characters long token, client must save this token for
	 *         later interactions with server.
	 * @throws XmlRpcException
	 *             Possible codes: {@link ResponseCode #SUCCESS SUCCESS},
	 *             {@link ResponseCode #PASSWD_INCORRECT PASSWD_INCORRECT}
	 *             {@link ResponseCode #VERSION_NOT_SUPPORTED
	 *             VERSION_NOT_SUPPORTED} {@link ResponseCode #SYSTEM_ERROR
	 *             SYSTEM_ERROR} {@link ResponseCode #SYSTEM_STOP SYSTEM_STOP}
	 */
	@Override
	public String login(String email, String password, String machineName,
			int version) throws XmlRpcException {
		logger.info(String.format("Request to login. Email: %s", email));
		Statistic.getInstance().increRequestCount();

		// User Authenticate
		User user = UserFactory.getInstance().getUser(email);
		if (user != null && user.checkPassword(password)) {
			// Password correct
			Token token = TokenFactory.getInstance().genToken(user);

			assist.checkUserFirstShowUp(user);
			logger.info(String.format(
					"User login successfully. Email: %s, Token:%s", email,
					token));
			return token.getTokenString();
		} else {
			// Or not
			logger.info(String.format(
					"User login failed. Email: %s, Password: %s", email,
					password));
			throw new XmlRpcException(ResponseCode.PASSWD_INCORRECT,
					ErrorTips.PASSWD_INCORRECT);
		}
	}

	/**
	 * This is a deprecated version of
	 * {@link #login(String, String, String, int)}
	 */
	@Override
	@Deprecated
	public String login(String email, String password) throws XmlRpcException {
		return this.login(email, password, "", 0);
	}

	/**
	 * Client could use this function to create file
	 * 
	 * @param token
	 *            The 128-characters long token got when login.
	 * @param path
	 *            The path of file to be created, should be exactly like
	 *            /MySpace/testFile1, etc, a parent directory must exist for the
	 *            file.
	 * @return Nothing, means success, otherwise exceptions will be thrown.
	 * @throws XmlRpcException
	 *             Possible codes: {@link ResponseCode #TOKEN_INCORRECT
	 *             TOKEN_INCORRECT} {@link ResponseCode #TOKEN_TIMEOUT
	 *             TOKEN_TIMEOUT} {@link ResponseCode #SYSTEM_ERROR
	 *             SYSTEM_ERROR} {@link ResponseCode #SYSTEM_STOP SYSTEM_STOP}
	 */
	@Override
	public int mkFile(String token, String path) throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String.format("Request to make file. Email: %s, Path: %s",
				handler.getUser().getEmail(), handler.getPath()
						.getRealPathString()));
		handler.checkWritable();

		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			// check for available space.
			long capacity[] = assist.getPathCapacity(realPath);
			// capacity[0] is whole capacity
			// capacity[1] is used.
			if (capacity[0] < capacity[1]) {
				throw new XmlRpcException(ResponseCode.SPACE_FULL,
						"You don't have enough space on the target path.");
			}

			try {
				FileObject hylaFile = assist.getHylaFileSystem().openObject(
						realPath);
				int ret = assist.handleHylaOS(hylaFile
						.createFile(new DefaultDistributionPolicy()));

				// No need to set parent time meta for now.
				// Need to before, when meepo and hyla are pieces of ****.
				return ret;

			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().mkFile(token, path);
		}
	}

	/**
	 * Client could use this function to create directories.
	 * 
	 * @param token
	 *            The 128-characters long token got when login.
	 * @param path
	 *            The path of file to be created, should be exactly like
	 *            /MySpace/testDirectory, etc, a parent directory must exist for
	 *            the new directory.
	 * @return Nothing, means success, otherwise exceptions will be thrown.
	 * @throws XmlRpcException
	 *             Possible codes: {@link ResponseCode #TOKEN_INCORRECT
	 *             TOKEN_INCORRECT} {@link ResponseCode #TOKEN_TIMEOUT
	 *             TOKEN_TIMEOUT} {@link ResponseCode #SYSTEM_ERROR
	 *             SYSTEM_ERROR} {@link ResponseCode #SYSTEM_STOP SYSTEM_STOP}
	 */
	@Override
	public int mkDir(String token, String path) throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String.format("Request to make dir. Email: %s, Path: %s",
				handler.getUser().getEmail(), handler.getPath()
						.getRealPathString()));
		handler.checkWritable();
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			FileObject hylaFile = assist.getHylaFileSystem().openObject(
					realPath);

			// Set access control just like parent;
			try {
				assist.handleHylaOS(hylaFile.makeDirectories());
				FileObject parent = hylaFile.getParent();
				MeepoExtMeta extMeta = (MeepoExtMeta) parent.getExtendedMeta();
				if (extMeta == null) {
					extMeta = new MeepoExtMeta();
					parent.putExtendedMeta(extMeta);
				}
				int accessMask = extMeta.getPermission();
				extMeta = new MeepoExtMeta();
				extMeta.setPermission(accessMask);
				hylaFile.putExtendedMeta(extMeta);
			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
			return ResponseCode.SUCCESS;
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().mkDir(token, path);
		}
	}

	/**
	 * A deprecated version of {@link #delete(String, String)}.
	 */
	@Deprecated
	public int rmFile(String token, String path) throws XmlRpcException {
		return delete(token, path);
	}

	/**
	 * A deprecated version of {@link #delete(String, String)}.
	 */
	@Deprecated
	public int rmDir(String token, String path) throws XmlRpcException {
		return this.delete(token, path);
	}

	@Override
	public int delete(String token, String path) throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String.format("Request to delete. Email: %s, Path: %s",
				handler.getUser().getEmail(), handler.getPath()
						.getRealPathString()));
		handler.checkWritable();
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			try {
				FileObject hylaFile = assist.getHylaFileSystem().openObject(
						realPath);
				if (!path.startsWith(MeepoAssist.TRASH_PREFIX)) {
					// Abandon delete, move it to a temporary location
					// create parent if not exists
					String parentPath = "/.Trash/"
							+ hylaFile.getParent().getPath();
					FileObject parentDir = assist.getHylaFileSystem()
							.openObject(parentPath);
					if (!parentDir.exists()) {
						parentDir.makeDirectories();
					}
					String newFileName = parentDir.getPath() + "/"
							+ System.currentTimeMillis() + "."
							+ hylaFile.getName();
					FileObject newFile = assist.getHylaFileSystem().openObject(
							newFileName);
					hylaFile.moveTo(newFile);
				} else {
					assist.handleHylaOS(hylaFile.delete());
				}

				return ResponseCode.SUCCESS;
			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						ErrorTips.SYSTEM_ERROR);
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().delete(token, path);
		}
	}

	@Override
	@Deprecated
	public Object[] listDir(String token, String path) throws XmlRpcException {
		Object[] tmpObjs = this.listDirMaps(token, path);
		Object[] retObjs = new Object[tmpObjs.length];
		for (int i = 0; i < tmpObjs.length; i++) {
			@SuppressWarnings("unchecked")
			Map<Object, Object> m = (Map<Object, Object>) tmpObjs[i];
			AttributeInfo ai = new AttributeInfo(m);
			retObjs[i] = ai.toDeprecatedString();
		}
		return retObjs;
	}

	@Override
	@Deprecated
	public Object[] listDirWithSha1AndVersion(String token, String path)
			throws XmlRpcException {
		Object[] tmpObjs = this.listDirMaps(token, path);
		Object[] retObjs = new Object[tmpObjs.length];
		for (int i = 0; i < tmpObjs.length; i++) {
			@SuppressWarnings("unchecked")
			Map<Object, Object> m = (Map<Object, Object>) tmpObjs[i];
			AttributeInfo ai = new AttributeInfo(m);
			retObjs[i] = ai.toDeprecatedStringWithSha1AndVersion();
		}
		return retObjs;
	}

	@Override
	public Object[] listRoot(String token, String lang) throws XmlRpcException {
		// logger.info(String.format("Request to list Root. Email: %s", email));
		Statistic.getInstance().increRequestCount();

		String[] retStrings;
		String s0;
		String s1;
		String s2;
		long c = System.currentTimeMillis() / 1000;
		// if(lang.equalsIgnoreCase("zh-CN")){
		// s0 = String.format("%s:%s:d:%d:%d:%d:4096", MeepoAssist.MYSPACE,
		// MeepoAssist.MYSPACE, c, c, c);
		// s1 = String.format("%s:%s:d:%d:%d:%d:4096", MeepoAssist.GROUPS,
		// MeepoAssist.GROUPS, c, c, c);
		// s2 = String.format("%s:%s:d:%d:%d:%d:4096", MeepoAssist.PUBLIC,
		// MeepoAssist.PUBLIC, c, c, c);
		// s0 = MeepoAssist.MYSPACE + ":个人空间:d:0:0:0:4096";
		// s1 = MeepoAssist.GROUPS + ":社区空间:d:0:0:0:4096";
		// s2 = MeepoAssist.PUBLIC + ":公共社区:d:0:0:0:4096";
		// }else{
		s0 = String.format("%s:%s:d:%d:%d:%d:4096", MeepoAssist.MYSPACE,
				MeepoAssist.MYSPACE, c, c, c);
		s1 = String.format("%s:%s:d:%d:%d:%d:4096", MeepoAssist.GROUPS,
				MeepoAssist.GROUPS, c, c, c);
		s2 = String.format("%s:%s:d:%d:%d:%d:4096", MeepoAssist.PUBLIC,
				MeepoAssist.PUBLIC, c, c, c);
		// }
		retStrings = new String[] { s0, s1, s2 };
		return retStrings;
	}

	@Override
	public int move(String token, String oldPath, String newPath, boolean force)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, oldPath);
		PreProcessHandler anotherHandler = new PreProcessHandler(token, newPath);
		handler.checkWritable();
		anotherHandler.checkWritable();
		if (!handler.getPath().getDomain()
				.equals(anotherHandler.getPath().getDomain())) {
			throw new XmlRpcException(ResponseCode.MOVE_NOT_SAME_PARTITION,
					ErrorTips.MOVE_NOT_SAME_PARTITION);
		}
		if (handler.isLocal()) {
			// Handle this locally
			String realOldPath = handler.getPath().getRealPathString();
			String realNewPath = anotherHandler.getPath().getRealPathString();

			logger.info(String.format("Move %s to %s.Email:%s.", oldPath,
					newPath, handler.getUser().getEmail()));

			FileObject oldFile = assist.getHylaFileSystem().openObject(
					realOldPath);
			FileObject newFile = assist.getHylaFileSystem().openObject(
					realNewPath);

			try {
				// if force to move and not just case different
				if (newFile.exists() && force
						&& !realOldPath.equalsIgnoreCase(realNewPath)) {
					newFile.delete();
				}

				int ret = assist.handleHylaOS(oldFile.moveTo(newFile));
				PermissionFixer pf = PermissionFixer.getInstance();
				int a = pf.getPermission(newFile.getParent());
				pf.setPermission(newFile, a);
				pf.addDir(newFile);

				// No need to change modify time of parent directories.
				return ret;

			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						ErrorTips.SYSTEM_ERROR);
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().move(token, oldPath, newPath);
		}
	}

	@Override
	@Deprecated
	public int forceMove(String token, String oldPath, String newPath)
			throws XmlRpcException {
		return this.move(token, oldPath, newPath, true);
	}

	@Override
	@Deprecated
	public int move(String token, String oldPath, String newPath)
			throws XmlRpcException {
		return this.move(token, oldPath, newPath, false);
	}

	@Override
	@Deprecated
	public int setAttr(String token, String path, String sizeInString,
			String ctimeInSecondString, String mtimeInSecondString,
			String atimeInSecondString) throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			if (!handler.isWritable()) {
				throw new XmlRpcException(ResponseCode.PERMISSION_DENIED,
						ErrorTips.PERMISSION_DENIED);
			}
			String realPath = handler.getPath().getRealPathString();
			FileObject hylaFile = assist.getHylaFileSystem().openObject(
					realPath);
			logger.debug("Trying to set attr to " + realPath);
			try {
				Long length = Long.parseLong(sizeInString);
				Long ctime = Long.parseLong(ctimeInSecondString);
				Long mtime = Long.parseLong(mtimeInSecondString);
				Long atime = Long.parseLong(atimeInSecondString);
				Meta meta = hylaFile.getMeta();

				if (length >= 0)
					meta.setSize(length);
				if (ctime > 100)
					meta.setCreateTime(ctime * 1000);
				else if (ctime >= 0)
					meta.setCreateTime(new Date().getTime());

				if (mtime > 100)
					meta.setModifyTime(mtime * 1000);
				else if (mtime >= 0)
					meta.setModifyTime(new Date().getTime());

				if (atime > 100)
					meta.setAccessTime(atime * 1000);
				else if (atime >= 0)
					meta.setAccessTime(new Date().getTime());
				hylaFile.updateMeta(meta);
				return ResponseCode.SUCCESS;

			} catch (NumberFormatException e) {
				throw new XmlRpcException(ResponseCode.INVALID_OPERATION,
						"Parameter Wrong.");
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().setAttr(token, path,
					sizeInString, ctimeInSecondString, mtimeInSecondString,
					atimeInSecondString);
		}
	}

	/**
	 * Get attributes of a give file or directory, which includes file name,
	 * size, time information, etc.
	 * 
	 * @param token
	 * @param path
	 * @return A map (struct in xml-rpc) contains all the attributes, which
	 *         contains the following key-value reflection. <br>
	 *         name, string<br>
	 *         is_dir, boolean<br>
	 *         create_time, i8<br>
	 *         modify_time, i8<br>
	 *         access_time, i8<br>
	 *         replica_count, i4<br>
	 *         sha1, string<br>
	 *         version, i4<br>
	 *         snapshot, i8<br>
	 */
	@Override
	public Map<Object, Object> getAttrMap(String token, String path)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String.format(
				"Request to list attributes map. Email: %s, Path: %s", handler
						.getUser().getEmail(), handler.getPath()
						.getRealPathString()));
		if (handler.isLocal()) {
			FileObject file = assist.getHylaFileSystem().openObject(
					handler.getPath().getRealPathString());
			try {
				if (!file.exists()) {
					throw new IOException();
				}
			} catch (IOException e) {
				logger.debug("User try to get attr of not existing file." + " "
						+ handler.getPath().getRealPathString());
				throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
						ErrorTips.FILE_DIR_NOT_EXISTS);
			}

			AttributeInfo attrInfo = new AttributeInfo(file);
			return attrInfo.toMap();

		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().getAttrMap(token, path);
		}
	}

	/**
	 * You should use {@link #getAttrMap(String, String)} instead of this.
	 */
	@Override
	@Deprecated
	public String getAttr(String token, String path) throws XmlRpcException {
		Object o = this.getAttrMap(token, path);

		@SuppressWarnings("unchecked")
		Map<Object, Object> m = (Map<Object, Object>) o;
		AttributeInfo ai = new AttributeInfo(m);
		return ai.toDeprecatedString();
	}

	@Override
	public int setAccessControl(String token, String path, int accessMask)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			String realPath = handler.getPath().getRealPathString();
			FileObject hylaFile = assist.getHylaFileSystem().openObject(
					realPath);
			try {
				if (!hylaFile.isDirectory())
					throw new XmlRpcException(
							ResponseCode.ACCESS_NOT_DIRECTORY,
							"The path is not a directory.");

				// Get group name from this path.
				User user = handler.getUser();
				Group g = new Path(path, user).extractGroup();

				// The user has to be administrator of this group.
				if (g == null || !user.isAdminOfGroup(g))
					throw new XmlRpcException(ResponseCode.ACCESS_NOT_ADMIN,
							"You have no right to set access control.");

				// This should be successful, unless there is a unknown error.
				PermissionFixer pf = PermissionFixer.getInstance();
				pf.setPermission(hylaFile, accessMask);
				pf.addDir(hylaFile);
				return ResponseCode.SUCCESS;
			} catch (IOException e1) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().mkFile(token, path);
		}
	}

	@Override
	public int getAccessControl(String token, String path)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);

		if (handler.isLocal()) {
			// Any user can get this access control info.
			String realPath = handler.getPath().getRealPathString();
			FileObject hylaFile = assist.getHylaFileSystem().openObject(
					realPath);
			int accessMask = PermissionFixer.getInstance().getPermission(
					hylaFile);
			return accessMask;
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().getAccessControl(token, path);
		}
	}

	@Override
	public int setReplicaNumber(String token, String path, int replicaNumber)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String
				.format("Request to setReplicaNumber.Email:%s. path:%s. replicaNumber:%d.",
						handler.getUser().getEmail(), path, replicaNumber));
		if (handler.isLocal()) {
			String realPath = handler.getPath().getRealPathString();
			FileObject hylaFile = assist.getHylaFileSystem().openObject(
					realPath);
			try {
				if (!hylaFile.isDirectory())
					throw new XmlRpcException(
							ResponseCode.ACCESS_NOT_DIRECTORY,
							"The path is not a directory.");

				// This should be successful, unless there is a unknown error.
				MeepoExtMeta extMeta = (MeepoExtMeta) hylaFile
						.getExtendedMeta();
				if (extMeta == null)
					extMeta = new MeepoExtMeta();
				extMeta.setReplicaNumber(replicaNumber);
				hylaFile.putExtendedMeta(extMeta);
				return ResponseCode.SUCCESS;
			} catch (IOException e1) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().setAccessControl(token, path,
					replicaNumber);
		}
	}

	@Override
	public int getReplicaNumber(String token, String path)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			// Any user can get this access control info.
			String realPath = handler.getPath().getRealPathString();
			try {
				FileObject hylaFile = assist.getHylaFileSystem().openObject(
						realPath);
				if (!hylaFile.isDirectory()) {
					throw new XmlRpcException(
							ResponseCode.ACCESS_NOT_DIRECTORY,
							"The path is not a directory.");
				}
				MeepoExtMeta exMeta = (MeepoExtMeta) hylaFile.getExtendedMeta();
				int replicaNumber = 1;
				if (exMeta != null)
					replicaNumber = exMeta.getReplicaNumber();
				return replicaNumber;

			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().getAccessControl(token, path);
		}
	}

	@Override
	@Deprecated
	public String getRootAttr(String token) throws XmlRpcException {
		return getAttr(token, MeepoAssist.SLASH);
	}

	@Override
	public Object[] downloadRequest(String token, String path)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			FileObject file = assist.getHylaFileSystem().openObject(
					handler.getPath().getRealPathString());
			try {
				Distribution dst = file.getFileDistribution();
				if (dst == null) {
					throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
							"");
				}
				DataSegment[] dsg = dst.getDataSegments();
				Storage storage = assist.getHylaFileSystem().getStorage(
						dsg[0].getStorageId());
				if (storage == null) {
					throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
							"System error on gaining storage information");
				}
				String base = storage.getURL();
				String filePath = MeepoAssist.SLASH + dsg[0].getPathOnStorage();
				// String date = new SimpleDateFormat ("yyyyMMdd").format(new
				// Date());
				// String sha1 = CommonUtil.SHA1(filePath + date +
				// Environment.MAGIC_STRING);
				logger.info(String.format("Download File %s",
						(handler.getPath().getRealPathString())));
				FileDistInfo info = new FileDistInfo(StorageEnum.HG, base
						+ filePath, 0L, file.getMeta().getSize());
				return new Object[] { info.toMap() };
			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Old_TODO
			return null;
		}
	}

	@Override
	public Object[] readFileEncrypted(String token, String path)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		boolean isWritable = handler.isWritable();
		if (handler.isLocal()) {
			String realPath = handler.getPath().getRealPathString();
			FileObject file = assist.getHylaFileSystem().openObject(realPath);
			try {
				Distribution dst = file.getFileDistribution();
				if (dst == null) {
					throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
							"");
				}
				DataSegment[] dsg = dst.getDataSegments();
				Storage storage = assist.getHylaFileSystem().getStorage(
						dsg[0].getStorageId());
				if (storage == null) {
					throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
							"System error on gaining storage information");
				}
				String base = storage.getURL();
				String filePath = MeepoAssist.SLASH + dsg[0].getPathOnStorage();
				String date = new SimpleDateFormat("yyyyMMdd")
						.format(new Date());
				String magic = isWritable ? "123" : "456";
				String sha1 = CommonUtil.SHA1(filePath + date + magic);
				logger.info(String.format("Read File %s.Email:%s.Ip:%s.",
						realPath, handler.getUser().getEmail(),
						MeepoServlet.getClientIpAddress()));
				return new Object[] { base, filePath, sha1 };
			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().readFileEncrypted(token, path);
		}
	}

	@Override
	public Object[] writeFileEncryptedWithExpectedSize(String token,
			String path, int lockTime, String expectedSizeInLong)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		handler.checkWritable();
		if (handler.isLocal()) {
			// Don't play with lock time now.
			String realPath = handler.getPath().getRealPathString();
			// Check with access control
			FileObject file = assist.getHylaFileSystem().openObject(realPath);
			try {
				Distribution dst = file.getFileDistribution();
				if (dst == null) {
					throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
							"");
				}
				DataSegment[] dsg = dst.getDataSegments();
				Storage storage = assist.getHylaFileSystem().getStorage(
						dsg[0].getStorageId());
				String base = storage.getURL();
				String filePath = MeepoAssist.SLASH + dsg[0].getPathOnStorage();
				String date = new SimpleDateFormat("yyyyMMdd")
						.format(new Date());
				String sha1 = CommonUtil.SHA1(filePath + date + "123");
				logger.info(String.format("Read File %s.Email:%s.", realPath,
						handler.getUser().getEmail()));
				return new Object[] { base, filePath, sha1 };
			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						"System error.");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy()
					.writeFileEncryptedWithExpectedSize(token, path, lockTime,
							expectedSizeInLong);
		}
	}

	@Deprecated
	@Override
	public Object[] writeFileEncrypted(String token, String path, int lockTime)
			throws XmlRpcException {
		return this.writeFileEncryptedWithExpectedSize(token, path, lockTime,
				"-1");
	}

	/**
	 * 
	 * @param token
	 * @param path
	 * @return
	 * @throws XmlRpcException
	 */
	@Override
	@Deprecated
	public int writeConfirm(String token, String path, String sizeInString)
			throws XmlRpcException {
		return this.writeConfirmWithSha1AndVersion(token, path, sizeInString,
				"", -1);
	}

	@Override
	@Deprecated
	public int writeConfirmWithSha1AndVersion(String token, String path,
			String sizeInString, String sha1, int version)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String.format(
				"Request to writeConfirmWithSha1AndVersion.Email:%s. path:%s",
				handler.getUser().getEmail(), path));
		if (handler.isLocal()) {
			if (!handler.isWritable()) {
				// return ResponseCode.SUCCESS;
				throw new XmlRpcException(ResponseCode.PERMISSION_DENIED,
						"Permission Denied!");
			}
			String realPath = handler.getPath().getRealPathString();
			FileObject hylaFile = assist.getHylaFileSystem().openObject(
					realPath);
			try {
				Meta meta = hylaFile.getMeta();
				meta.setModifyTime(new Date().getTime());
				Long size = Long.parseLong(sizeInString);
				meta.setSize(size);
				hylaFile.updateMeta(meta);
				MeepoExtMeta extMeta = (MeepoExtMeta) hylaFile
						.getExtendedMeta();
				if (extMeta == null)
					extMeta = new MeepoExtMeta();
				if (version >= 0) {
					extMeta.setVersion(version);
				}
				if (!sha1.trim().isEmpty()) {
					extMeta.setSha1(sha1);
				}
				hylaFile.putExtendedMeta(extMeta);

				hylaFile.pushSnapshot();
			} catch (NumberFormatException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.INVALID_OPERATION,
						"Size format incorrect, must be a number in string!");
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().writeConfirmWithSha1AndVersion(
					token, path, sizeInString, sha1, version);
		}
		return ResponseCode.SUCCESS;
	}

	public int uploadComfirm(String token, String path,
			Map<Object, Object> attrMap) {
		// Old_TODO
		return 0;
	}

	@Override
	public Object[] getSha1AndVersion(String token, String path)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info("Request to get sha1 and version: "
				+ handler.getUser().getEmail()
				+ handler.getPath().getRealPathString());

		if (handler.isLocal()) {
			String realPath = handler.getPath().getRealPathString();
			Object[] retObjs = new Object[] { new String(""), new Integer(-1) };
			FileObject file = assist.getHylaFileSystem().openObject(realPath);
			try {
				MeepoExtMeta extMeta = (MeepoExtMeta) file.getExtendedMeta();
				if (extMeta != null) {
					retObjs[0] = extMeta.getSha1();
					retObjs[1] = extMeta.getVersion();
				}
			} catch (IOException e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			} /*
			 * catch (FileObjectNotExistsException e) { throw new
			 * XmlRpcException(MeepoRes.FILE_DIR_NOT_EXISTS, ""); }
			 */
			return retObjs;
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().getSha1AndVersion(token, path);
		}
	}

	/**
	 * For now we only check write access 1. If path is MySpace, then true; 2.
	 * If path is GroupSpace, we need to check
	 */
	@Override
	public int logout(String tokenString) throws XmlRpcException {
		// Remove from cache first
		Token token = TokenFactory.getInstance().getToken(tokenString);
		if (token != null) {
			TokenFactory.getInstance().purgeToken(token);
		}
		return ResponseCode.SUCCESS;
	}

	@Override
	public int rmDirPosix(String token, String path) throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			logger.info(String.format(
					"Delete dir %s with Posix semantics.Email:%s.", realPath,
					handler.getUser().getEmail()));
			FileObject dir = assist.getHylaFileSystem().openObject(realPath);
			try {
				if (!dir.exists())
					throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
							"");
				if (dir.list().length > 0)
					throw new XmlRpcException(ResponseCode.DIR_NOT_EMPTY, "");
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
			return delete(token, path);
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().rmDirPosix(token, path);
		}
		// This return should never be called.
	}

	/**
	 * List attributes maps of each sub-folder or sub-directory of the target
	 * directory.
	 * 
	 * @param token
	 * @param path
	 * @return
	 * @see #getAttrMap(String, String)
	 */
	@Override
	public Object[] listDirMaps(String token, String path)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			logger.info(String.format(
					"Request to list dir Maps. Email: %s, Path: %s", handler
							.getUser().getEmail(), handler.getPath()
							.getRealPathString()));
			// Handle this locally
			Path p = handler.getPath();
			String realPath = p.getRealPathString();
			ArrayList<Object> al = new ArrayList<Object>();
			// It's different to list MySpace and Groups space.
			try {
				if (p.isGroupRootPath() || p.isPublicRootPath()) {
					// Check for the user-group relationship.
					Group.Type t = p.isGroupRootPath() ? Type.REGULAR
							: Type.PUBLIC;
					Iterable<Group> groups = handler.getUser()
							.getJoinedGroupRelations(t);
					for (Group g : groups) {
						String groupName = g.getName();
						FileObject groupDir = assist.getHylaFileSystem()
								.openObject(
										MeepoAssist.TEMP_PREFIX_SUFFIX
												+ groupName);
						// Create if group has never been created.
						assist.checkGroupFirstShowUp(g);
						// Notice, these folders are just fake to list. other
						// requests such as read and write would be forwarded to
						// the Hyla master node
						groupDir.makeDirectory();
						if (groupDir.getAttributes() != null) {
							al.add(new AttributeInfo(groupDir).toMap());
						}
					}
					return al.toArray();
				} else if (p.isMySpacePath() || p.isGroupOrPublicPath()
						|| p.isRoot() || handler.getUser().isRoot()) {
					FileObject hylaDir = assist.getHylaFileSystem().openObject(
							realPath);
					FileObject[] files = hylaDir.list();
					ArrayList<Object> retObjects = new ArrayList<Object>();
					for (int i = 0; i < files.length; i++) {
						if (files[i].getAttributes() != null) {
							retObjects.add(new AttributeInfo(files[i]).toMap());
						}
					}
					return retObjects.toArray();
				} else {

					throw new XmlRpcException(ResponseCode.PERMISSION_DENIED,
							"");
				}
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().listDirMaps(token, path);
		}
	}

	/**
	 * Consider using {@link #listDirMaps(String, String)} instead.
	 */
	@Override
	@Deprecated
	public Object[] listDirWithSha1AndVersionAndChildDirs(String token,
			String path) throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);

		logger.info(String
				.format("Request to listDirWithSha1AndVersionAndChildDirs. Email: %s, Path: %s",
						handler.getUser().getEmail(), handler.getPath()
								.getRealPathString()));
		if (handler.isLocal()) {
			// Handle this locally
			Path p = handler.getPath();
			// It's only appropriate to call this on MySpace.
			try {
				if (p.isMySpacePath()) {
					Queue<FileObject> listQueue = new LinkedList<FileObject>();
					FileObject rootDir = assist.getHylaFileSystem().openObject(
							p.getRealPathString());
					listQueue.add(rootDir);
					LinkedList<String> al = new LinkedList<String>();
					while (!listQueue.isEmpty()) {
						FileObject hylaDir = listQueue.remove();
						FileObject childFiles[] = hylaDir.list();

						for (int i = 0; i < childFiles.length; i++) {
							if (childFiles[i].getAttributes() != null) {
								AttributeInfo ai = new AttributeInfo(
										childFiles[i]);
								Path tmpP = new Path(
										childFiles[i].getRealPath());
								String attrString = ai
										.toDeprecatedStringWithSha1AndVersion()
										+ ":" + tmpP.getFakePathString();
								al.add(attrString);
								if (childFiles[i].isDirectory()) {
									listQueue.add(childFiles[i]);
								}
							}
						}
					}
					return al.toArray(new String[al.size()]);
				} else {
					throw new XmlRpcException(ResponseCode.INVALID_OPERATION,
							"You can not call this on folders other than MySpace.");
				}
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						ErrorTips.SYSTEM_ERROR);
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy()
					.listDirWithSha1AndVersionAndChildDirs(token, path);
		}
	}

	@Override
	public int lockUser(String tokenString, int lockTimeInSecond)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(tokenString,
				Path.MYSPACE_PREFIX);

		if (handler.isLocal()) {
			boolean ret = assist.lockUser(handler.getToken(), lockTimeInSecond
					+ System.currentTimeMillis() / 1000);
			if (ret) {
				return ResponseCode.SUCCESS;
			} else {
				throw new XmlRpcException(ResponseCode.LOCK_CONFLICT,
						"Another user may be using this lock");
			}
		} else {
			return handler.getRemoteRpcProxy().lockUser(tokenString,
					lockTimeInSecond);
		}
	}

	@Override
	public int unlockUser(String tokenString) throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(tokenString,
				Path.MYSPACE_PREFIX);

		if (handler.isLocal()) {
			boolean ret = assist.unlockUser(handler.getToken());
			if (ret) {
				return ResponseCode.SUCCESS;
			} else {
				throw new XmlRpcException(ResponseCode.LOCK_CONFLICT,
						"You cannot unlock cause another user is using this and it has not expired.");
			}
		} else {
			return handler.getRemoteRpcProxy().unlockUser(tokenString);
		}

	}

	@Override
	public int getLockTime(String token) throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token,
				Path.MYSPACE_PREFIX_SUFFIX);
		Long expireTime = assist
				.getLockExpireTime(handler.getUser().getEmail());
		int lockTime;
		if (expireTime == null) {
			lockTime = 0;
		} else {
			lockTime = (int) (expireTime - System.currentTimeMillis()) / 1000;
		}
		return lockTime;
	}

	@Override
	public Object[] listAllReplicatedDirs(String token, String path)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			// It's different to list MySpace and Groups space.
			try {
				logger.debug("Start to list, the whole version." + realPath);

				if (realPath.startsWith(MeepoAssist.MYSPACE_PREFIX)) {
					FileObject rootDir = assist.getHylaFileSystem().openObject(
							realPath);
					LinkedList<String> ll = new LinkedList<String>();
					FileObject childFiles[] = rootDir.list();
					for (int i = 0; i < childFiles.length; i++) {
						FileObject childFile = childFiles[i];
						if (childFile.isDirectory()) {
							MeepoExtMeta extMeta = (MeepoExtMeta) childFile
									.getExtendedMeta();
							if (extMeta == null) {
								extMeta = new MeepoExtMeta();
							}
							Path p = new Path(childFile.getRealPath());
							String s = p.getFakePathString() + ":"
									+ extMeta.getReplicaNumber();
							ll.add(s);
						}
					}
					return ll.toArray();
				} else {
					throw new XmlRpcException(ResponseCode.INVALID_OPERATION,
							"You can not call this on folders other than MySpace.");
				}
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().listAllReplicatedDirs(token,
					path);
		}
	}

	@Override
	public Object[] listAllAccessControlDirs(String token, String path)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			// It's different to list MySpace and Groups space.
			try {
				logger.debug("Start to list, the whole version." + realPath);

				if (realPath.startsWith(MeepoAssist.GROUP_PREFIX)) {
					FileObject rootDir = assist.getHylaFileSystem().openObject(
							realPath);
					LinkedList<String> al = new LinkedList<String>();
					FileObject childFiles[] = rootDir.list();

					for (int i = 0; i < childFiles.length; i++) {
						FileObject childFile = childFiles[i];
						childFile.getPath();
						if (childFile.isDirectory()) {
							MeepoExtMeta extMeta = (MeepoExtMeta) childFile
									.getExtendedMeta();
							if (extMeta == null) {
								extMeta = new MeepoExtMeta();
								childFile.putExtendedMeta(extMeta);
							}
							int accessMask = extMeta.getPermission();
							Path p = new Path(childFile.getRealPath());
							String s = p.getFakePathString() + ":" + accessMask;
							al.add(s);
						}
					}
					return al.toArray();
				} else {
					throw new XmlRpcException(ResponseCode.INVALID_OPERATION,
							"You can not call this on folders other than Group space.");
				}
			} catch (IOException e) {
				logger.error("", e);
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR, "");
			}
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().listAllAccessControlDirs(token,
					path);
		}
	}

	@Override
	public int getUserGroupRelation(String token, String groupName)
			throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token,
				Path.GROUP_PREFIX_SUFFIX + groupName);
		User user = handler.getUser();
		Group group = GroupFactory.getInstance().getGroup(groupName);

		if (group == null) {
			throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS, "");
		}

		UGRelation r = user.relationOf(group);

		switch (r.getRelation()) {
		case NONE:
			return ResponseCode.USER_GROUP_NONE;
		case ADMIN:
			return ResponseCode.USER_GROUP_ADMIN;
		case MEMBER:
			return ResponseCode.USER_GROUP_MEMEBER;
		default:
			return ResponseCode.USER_GROUP_NONE;
		}
	}

	@Override
	public Map<Object, Object> getCapacityMap(String token, String path)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			long retLong[] = assist.getPathCapacity(realPath);
			HashMap<Object, Object> map = new HashMap<Object, Object>();
			map.put(MeepoAssist.CAPACITY_TOTAL, retLong[0]);
			map.put(MeepoAssist.CAPACITY_USED, retLong[1]);
			return map;
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().getCapacityMap(token, path);
		}
	}

	@Override
	@Deprecated
	public Object[] getCapacityInfo(String token, String path)
			throws XmlRpcException {

		PreProcessHandler handler = new PreProcessHandler(token, path);
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			long longs[] = assist.getPathCapacity(realPath);
			return new Object[] { longs[0] + "", longs[1] + "" };
		} else {
			// Route this request to the master node for this user.

			return handler.getRemoteRpcProxy().getCapacityInfo(token, path);
		}
	}

	@Override
	public int restore(String token, String path) throws XmlRpcException {
		PreProcessHandler handler = new PreProcessHandler(token, path);
		logger.info(String.format("Request to restore. Email: %s, Path: %s",
				handler.getUser().getEmail(), handler.getPath()
						.getRealPathString()));
		handler.checkWritable();
		if (handler.isLocal()) {
			// Handle this locally
			String realPath = handler.getPath().getRealPathString();
			try {
				FileObject hylaFile = assist.getHylaFileSystem().openObject(
						realPath);
				if (path.startsWith("/.Trash")) {

					Ugly.getInstance().areyoukiddingme(realPath);
				} else {
					// assist.handleHylaOS(hylaFile.delete());
					logger.error(String
							.format("not trash, can not be restored. Email: %s, Path:%s",
									handler.getUser().getEmail(), handler
											.getPath().getRealPathString()));
				}

				return ResponseCode.SUCCESS;
			} catch (Exception e) {
				throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
						ErrorTips.SYSTEM_ERROR);
			}
		} else {
			// Route this request to the master node for this user.
			return handler.getRemoteRpcProxy().restore(token, path);
		}

	}

	@Override
	@Deprecated
	public String getServerTimeUTC() {
		Statistic.getInstance().increPulseCount();
		Statistic.getInstance().increRequestCount();
		return System.currentTimeMillis() + "";
	}

	@Override
	public Long getServerTime() {
		Statistic.getInstance().increRequestCount();
		return System.currentTimeMillis();
	}

	@Override
	// Object[0] : cipher id . Object[1] : cipher String.
	public Object[] getDataServerTokenCipher(long id) throws XmlRpcException {
		Cipher cipher = CipherManager.getInstance().getCipher(id);
		if (cipher != null) {
			return new Object[] { cipher.getCipherId(),
					cipher.getCipherString() };
		} else {
			logger.error(ErrorTips.CIPHER_NOT_EXIST + id);
			throw new XmlRpcException(ResponseCode.CIPHER_NOT_EXIST,
					ErrorTips.CIPHER_NOT_EXIST);
		}
	}

	@Override
	public Object[] getCurrentDataServerTokenCipher() throws XmlRpcException {
		Cipher cipher = CipherManager.getInstance().getCurrentCipher();
		if (cipher != null) {
			return new Object[] { cipher.getCipherId(),
					cipher.getCipherString() };
		} else {
			logger.error(ErrorTips.CIPHER_NOT_EXIST);
			throw new XmlRpcException(ResponseCode.CIPHER_NOT_EXIST,
					ErrorTips.CIPHER_NOT_EXIST);
		}
	}

	private static Logger logger = Logger.getLogger(RpcImpl.class);
	private static MeepoAssist assist = MeepoAssist.getInstance();
	// private TokenManager tokenManager = TokenManager.getInstance();

}
