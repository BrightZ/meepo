package org.meepo.fs;

import java.util.Map;

public class FileDistInfo extends Info {
	public FileDistInfo(StorageEnum sType, String aUrl, Long aStart, Long aEnd) {
		this.type = sType.ordinal();
		this.url = aUrl;
		this.start = aStart;
		this.end = aEnd;
	}

	public FileDistInfo(Map<Object, Object> map) {
		super(map);
	}

	@Override
	public String toString() {
		return String.format("Type:%d\tUrl:%s\tStart:%d\tEnd:%d", type, url,
				start, end);
	}

	private Integer type;
	private String url;
	private Long start;
	private Long end;
}
