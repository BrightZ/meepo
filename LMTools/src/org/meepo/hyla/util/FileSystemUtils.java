package org.meepo.hyla.util;

import java.util.ArrayList;

import org.meepo.hyla.FileSystem;

public class FileSystemUtils {

	public static String[] splitPath(String path)
			throws IllegalArgumentException {
		String[] segs = path.split(FileSystem.SEPARATOR);
		ArrayList<String> segArrayList = new ArrayList<String>();
		for (String seg : segs) {
			if (seg.length() > FileSystem.MAX_SEGMENT_LENGTH) {
				throw new IllegalArgumentException("one or more segments "
						+ "exceeds maximum segment length limit");
			}
			if (containsIllegalChar(seg)) {
				throw new IllegalArgumentException("one or more segments "
						+ "contains at least one illegal character from "
						+ FileSystem.ILLEGAL_CHARS);
			}
			if (seg.equals("")) {
				continue;
			}
			if (seg.equals(".")) {
				continue;
			}
			if (seg.equals("..")) {
				if (segArrayList.size() == 0) {
					continue;
				}
				segArrayList.remove(segArrayList.size() - 1);
				continue;
			}

			segArrayList.add(seg);
		}

		if (segArrayList.size() > FileSystem.MAX_NUMBER_OF_SEGMENTS) {
			throw new IllegalArgumentException("path contains too many "
					+ "segments, maximum " + FileSystem.MAX_NUMBER_OF_SEGMENTS
					+ " segments supported");
		}

		return segArrayList.toArray(new String[0]);
	}

	public static boolean containsIllegalChar(String segment) {
		for (CharSequence cs : FileSystem.ILLEGAL_CHAR_SEQUENCES) {
			if (segment.contains(cs)) {
				return true;
			}
		}
		return false;
	}
}
