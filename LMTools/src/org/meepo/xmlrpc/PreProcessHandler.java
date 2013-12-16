package org.meepo.xmlrpc;

import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;
import org.meepo.MeepoExtMeta;
import org.meepo.common.ErrorTips;
import org.meepo.common.ResponseCode;
import org.meepo.config.Environment;
import org.meepo.fs.Path;
import org.meepo.hyla.ExtendedMeta;
import org.meepo.hyla.FileObject;
import org.meepo.monitor.Statistic;
import org.meepo.server.Server;
import org.meepo.server.ServerFactory;
import org.meepo.user.Group;
import org.meepo.user.Token;
import org.meepo.user.TokenFactory;
import org.meepo.user.User;

public class PreProcessHandler {
	public PreProcessHandler(String tokenString, String path)
			throws XmlRpcException {
		// Do statistics
		this.startTime = System.currentTimeMillis();
		Statistic.getInstance().increRequestCount();

		do {
			this.token = TokenFactory.getInstance().getToken(tokenString);
			if (this.token == null) {
				logger.info("Incorrect token " + tokenString);
				break;
			}
			this.user = token.getOwnerAndRenew();
			if (this.user == null) {
				break;
			}
			this.path = new Path(path, user);
			return;
		} while (true);

		throw new XmlRpcException(ResponseCode.USER_NOT_EXISTS,
				ErrorTips.USER_NOT_EXISTS);
	}

	public boolean isWritable() {
		boolean ret = false;
		if (this.user.isRoot()) {
			ret = true;
		} else if (this.path.isMySpacePath()) {
			// For MySpace, no access check
			ret = true;
		} else if (this.path.isGroupOrPublicPath()) {
			Group g = this.path.extractGroup();
			if (g == null) {
				// Group does not exist
				ret = false;
			} else if (user.isAdminOfGroup(g)) {
				// If this is administrator of the group, then he can do
				// everything
				ret = true;
			} else {
				// Directory level access control
				FileObject hylaFile = MeepoAssist.getInstance()
						.getHylaFileSystem()
						.openObject(path.getRealPathString());

				FileObject parentFile = hylaFile.getParent();
				if (parentFile == null) {
					ret = false;
				} else {
					MeepoExtMeta extMeta = null;
					ExtendedMeta rawExtMeta = null;
					try {
						rawExtMeta = parentFile.getExtendedMeta();
					} catch (Exception e1) {
						ret = false;
					}
					if (rawExtMeta == null) {
						extMeta = new MeepoExtMeta();
					} else if (rawExtMeta instanceof MeepoExtMeta) {
						extMeta = (MeepoExtMeta) rawExtMeta;
					} else {
						ret = false;
					}
					int accessMask = extMeta.getPermission();
					ret = (accessMask == ResponseCode.ACCESS_WRITE_ALLOW);
				}
			}
		}

		return ret;
	}

	public boolean checkWritable() throws XmlRpcException {
		boolean ret = false;
		if (this.user.isRoot()) {
			ret = true;
		} else if (this.path.isMySpacePath()) {
			// For MySpace, no access check
			ret = true;
		} else if (this.path.isGroupOrPublicPath()) {
			Group g = this.path.extractGroup();
			if (g == null) {
				// Group does not exist
				ret = false;
			} else if (user.isAdminOfGroup(g)) {
				// If this is administrator of the group, then he can do
				// everything
				ret = true;
			} else {
				// Directory level access control
				FileObject hylaFile = MeepoAssist.getInstance()
						.getHylaFileSystem()
						.openObject(path.getRealPathString());

				FileObject parentFile = hylaFile.getParent();
				if (parentFile == null) {
					ret = false;
				} else {
					MeepoExtMeta extMeta;
					ExtendedMeta rawExtMeta;
					try {
						rawExtMeta = parentFile.getExtendedMeta();
					} catch (Exception e1) {
						throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
								ErrorTips.SYSTEM_ERROR);
					}
					if (rawExtMeta == null) {
						extMeta = new MeepoExtMeta();
					} else if (rawExtMeta instanceof MeepoExtMeta) {
						extMeta = (MeepoExtMeta) rawExtMeta;
					} else {
						throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
								ErrorTips.SYSTEM_ERROR);
					}
					int accessMask = extMeta.getPermission();
					ret = (accessMask == ResponseCode.ACCESS_WRITE_ALLOW);
				}
			}
		}

		if (!ret) {
			throw new XmlRpcException(ResponseCode.PATH_ACCESS_DENIED,
					"You have not right to change the file");
		}
		return ret;
	}

	public boolean isLocal() {
		if (this.user.getEmail().equalsIgnoreCase("meeposearch99713@meepo.org")) {
			return true;
		} else {
			return (path.getDomain().equals(Environment.getDomain()));
		}
	}

	public RpcInterface getRemoteRpcProxy() throws XmlRpcException {
		Server server = ServerFactory.getInstance().getServer(path.getDomain());
		// Old_TODO
		// Temporarily forbid cross domain access.

		throw new XmlRpcException(ResponseCode.USER_NOT_EXISTS,
				ErrorTips.USER_NOT_EXISTS);

		// String urlString = "http://" + server.getHostPlusPort() +
		// "/meepo/xmlrpc";
		// RpcInterface m =
		// RpcProxyFactory.getInstance().getMeepoImpl(urlString);
		// return m;
	}

	public Path getPath() {
		return path;
	}

	public User getUser() {
		return user;
	}

	public Token getToken() {
		return token;
	}

	public void close() {
		this.endTime = System.currentTimeMillis();

	}

	private Path path;
	private User user;
	private Token token;
	private long startTime;
	private long endTime;

	private static Logger logger = Logger.getLogger(PreProcessHandler.class);
}
