package org.meepo.fs;

import org.apache.log4j.Logger;
import org.meepo.config.Environment;
import org.meepo.user.Group;
import org.meepo.user.GroupFactory;
import org.meepo.user.User;

public class Path {
	public Path(String aFakePath, User aUser) {
		this(aFakePath, false, aUser);
	}

	public Path(String realPath) {
		this(realPath, true, null);
	}

	public Path(String path, boolean real, User aUser) {
		// format the path
		path = path.replace("//", "/");

		if (real) {
			this.realPath = path;
		} else {
			this.fakePath = path;
		}
		this.user = aUser;
	}

	public String getRealPathString() {
		if (this.realPath != null) {
			return realPath;
		} else {
			return this.fakeToReal();
		}
	}

	public String getFakePathString() {
		if (this.fakePath != null) {
			return fakePath;
		} else {
			return this.realToFake();
		}
	}

	private String realToFake() {
		this.fakePath = this.realPath;
		if (this.isMySpacePath()) {
			int secondPos = this.realPath.indexOf(SLASH, 1);
			if (secondPos > 0) {
				int thirdPos = this.realPath.indexOf(SLASH, secondPos + 1);
				if (thirdPos > 0) {
					this.fakePath = MYSPACE_PREFIX
							+ this.realPath.substring(thirdPos);
				} else {
					this.fakePath = this.realPath.substring(0, secondPos);
				}
			}
		}
		return this.fakePath;
	}

	private String fakeToReal() {
		// LOGGER.debug("Trying to convert path:" + fakePath);
		if (this.isMySpacePath()) {
			if (this.user == null) {
				return null;
			}
			this.realPath = MYSPACE_PREFIX_SUFFIX + this.user.getEmail()
					+ this.fakePath.substring(MYSPACE_PREFIX.length());
		} else {
			this.realPath = fakePath;
		}
		return realPath;
	}

	public boolean isRoot() {
		return this.getRealPathString().equals("/");
	}

	public boolean isMySpacePath() {
		if (this.isMySpace != null) {
			return isMySpace;
		}

		String s = realPath == null ? fakePath.toLowerCase() : realPath
				.toLowerCase();
		this.isMySpace = s.startsWith(MYSPACE_PREFIX.toLowerCase());
		return isMySpace;
	}

	public boolean isGroupOrPublicPath() {
		if (this.isGroupOrPublic != null) {
			return isGroupOrPublic;
		}
		String s = realPath == null ? fakePath.toLowerCase() : realPath
				.toLowerCase();
		isGroupOrPublic = s.startsWith(GROUP_PREFIX.toLowerCase())
				|| s.startsWith(PUBLIC_PREFIX.toLowerCase());
		return isGroupOrPublic;
	}

	public boolean isGroupRootPath() {
		if (isGroupRoot != null) {
			return isGroupRoot;
		}
		String s = this.getRealPathString().toLowerCase();
		this.isGroupRoot = (s.equals(GROUP_PREFIX.toLowerCase()) || s
				.equals(GROUP_PREFIX_SUFFIX.toLowerCase()));
		if (isGroupRoot) {
			this.isGroupOrPublic = true;
		}
		return this.isGroupRoot;
	}

	public boolean isPublicRootPath() {
		if (isPublicRoot != null) {
			return isPublicRoot;
		}

		String s = this.getRealPathString().toLowerCase();
		this.isPublicRoot = (s.equals(PUBLIC_PREFIX.toLowerCase()) || s
				.equals(PUBLIC_PREFIX_SUFFIX.toLowerCase()));

		if (isPublicRoot) {
			this.isGroupOrPublic = true;
		}
		return this.isPublicRoot;
	}

	public Group extractGroup() {
		if (this.group != null) {
			return this.group;
		}

		if (this.isMySpacePath()) {
			return null;
		}

		String path = this.getRealPathString();
		if (path.startsWith(GROUP_PREFIX) || path.startsWith(PUBLIC_PREFIX)) {
			int secondSlashPos = path.indexOf(SLASH, 0 + 1);
			int thirdSlashPos = path.indexOf(SLASH, secondSlashPos + 1);
			thirdSlashPos = thirdSlashPos > 0 ? thirdSlashPos : path.length();
			if (secondSlashPos < 0 || secondSlashPos == path.length() - 1) {
				return null;
			} else {
				String groupName = path.substring(secondSlashPos + 1,
						thirdSlashPos);
				LOGGER.debug("Break group name :" + groupName);
				this.group = GroupFactory.getInstance().getGroup(groupName);
				return this.group;
			}
		} else {
			return null;
		}
	}

	public Integer getDomain() {
		if (domain != null) {
			return domain;
		}

		if (this.isMySpacePath()) {
			// This is a private
			this.domain = user.getDomain();
		} else {
			// This is a group path, find the group domain
			if (this.isGroupRootPath() || this.isPublicRootPath()) {
				this.domain = Environment.getDomain();
			} else {
				this.extractGroup();
				if (this.group != null) {
					this.domain = group.getDomain();
				} else {
					this.domain = Environment.getDomain();
				}
			}
		}
		return this.domain;
	}

	private String realPath;
	private String fakePath;
	private User user;
	private Group group;
	private Integer domain;

	private Boolean isMySpace;
	private Boolean isGroupOrPublic;
	private Boolean isGroupRoot;
	private Boolean isPublicRoot;

	private static Logger LOGGER = Logger.getLogger(Path.class);

	public final static String SLASH = "/";

	public final static String MYSPACE = "MySpace";
	public final static String GROUPS = "Groups";
	public final static String PUBLIC = "Public";
	public final static String TEMP = ".tmp";

	public final static String MYSPACE_PREFIX = SLASH + MYSPACE;
	public final static String GROUP_PREFIX = SLASH + GROUPS;
	public final static String PUBLIC_PREFIX = SLASH + PUBLIC;
	public final static String TEMP_PREFIX = SLASH + TEMP;

	public final static String MYSPACE_PREFIX_SUFFIX = SLASH + MYSPACE + SLASH;
	public final static String GROUP_PREFIX_SUFFIX = SLASH + GROUPS + SLASH;
	public final static String PUBLIC_PREFIX_SUFFIX = SLASH + PUBLIC + SLASH;
	public final static String TEMP_PREFIX_SUFFIX = SLASH + TEMP + SLASH;

	public final static String GROUP_UPLOAD = "upload";
	public final static String GROUP_PUBLIC = "public";

}
