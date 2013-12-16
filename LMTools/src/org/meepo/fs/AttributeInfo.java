package org.meepo.fs;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;
import org.meepo.MeepoExtMeta;
import org.meepo.common.ErrorTips;
import org.meepo.common.ResponseCode;
import org.meepo.hyla.FileObject;
import org.meepo.hyla.Meta;

public class AttributeInfo extends Info {
	public AttributeInfo(Map<Object, Object> map) {
		super(map);
	}

	public AttributeInfo(FileObject object) throws XmlRpcException {
		try {
			Meta meta = object.getAttributes();
			if (meta == null) {
				LOGGER.debug("User try to get attr of not existing file." + " "
						+ object.getPath());
				throw new XmlRpcException(ResponseCode.FILE_DIR_NOT_EXISTS,
						ErrorTips.FILE_DIR_NOT_EXISTS);
			}
			MeepoExtMeta extMeta = (MeepoExtMeta) object.getExtendedMeta();
			if (extMeta == null) {
				extMeta = new MeepoExtMeta();
				object.putExtendedMeta(extMeta);
			}
			this.name = meta.getName();
			this.size = meta.getSize();
			this.isDir = meta.isDirectory();
			this.createTime = meta.getCreateTime();
			this.modifyTime = meta.getModifyTime();
			this.accessTime = meta.getAccessTime();
			this.snapshot = meta.getSnapshot();
			this.replicaCount = extMeta.getReplicaNumber();
			this.version = (long) extMeta.getVersion();
			this.sha1 = extMeta.getSha1();
			this.permission = extMeta.getPermission();

		} catch (IOException e) {
			LOGGER.error("", e);
			throw new XmlRpcException(ResponseCode.SYSTEM_ERROR,
					"System error.");
		}
	}

	@Override
	public String toString() {
		StringBuffer bf = new StringBuffer();
		bf.append(this.name);
		bf.append("\t");
		bf.append(this.size);
		bf.append("\t");
		bf.append(this.isDir);
		bf.append("\t");
		bf.append(this.createTime);
		bf.append("\t");
		bf.append(this.modifyTime);
		bf.append("\t");
		bf.append(this.accessTime);
		bf.append("\t");
		bf.append(this.sha1);
		bf.append("\t");
		bf.append(this.version);
		bf.append("\t");
		bf.append(this.snapshot);
		bf.append("\t");
		bf.append(this.replicaCount);
		bf.append("\t");
		bf.append(this.permission);
		return bf.toString();
	}

	@Deprecated
	public String toDeprecatedString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.name + ":");
		if (this.isDir) {
			sb.append("drwxrwxrwx" + ":");
		} else {
			sb.append("frwxrwxrwx" + ":");
		}
		sb.append("owner" + ":");
		sb.append(this.createTime / 1000 + ":");
		sb.append(this.modifyTime / 1000 + ":");
		sb.append(this.accessTime / 1000 + ":");
		sb.append(this.size + ":");
		return sb.toString();
	}

	@Deprecated
	public String toDeprecatedStringWithSha1AndVersion() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.name + ":");
		if (this.isDir) {
			sb.append("drwxrwxrwx" + ":");
		} else {
			sb.append("frwxrwxrwx" + ":");
		}
		sb.append("owner" + ":");
		sb.append(this.createTime / 1000 + ":");
		sb.append(this.modifyTime / 1000 + ":");
		sb.append(this.accessTime / 1000 + ":");
		sb.append(this.size + ":");
		sb.append(":");
		sb.append(this.sha1);
		sb.append(":");
		sb.append(this.version);
		return sb.toString();
	}

	public String name;
	public Long size;
	public Long createTime;
	public Long modifyTime;
	public Long accessTime;
	public String sha1;
	public Long version;
	public Long snapshot;
	public Integer replicaCount;
	public Boolean isDir;
	public Integer permission;

	private static final Logger LOGGER = Logger.getLogger(AttributeInfo.class);
}
