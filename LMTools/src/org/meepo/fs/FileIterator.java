package org.meepo.fs;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.meepo.hyla.FileObject;

public class FileIterator implements Iterator<FileObject> {

	public FileIterator(FileObject fo) {
		listQueue.add(fo);
	}

	@Override
	public boolean hasNext() {
		return (listQueue.size() > 0);
	}

	@Override
	public FileObject next() {
		FileObject nextFO = null;
		try {
			nextFO = listQueue.remove();
			if (nextFO.isDirectory()) {
				FileObject childFiles[] = nextFO.list();
				for (int i = 0; i < childFiles.length; i++) {
					listQueue.add(childFiles[i]);
				}
			}
			return nextFO;
		} catch (IllegalStateException e) {
			String name = "";
			if (nextFO != null) {
				name = nextFO.getName();
			}
			logger.error("Filename:" + name, e);
		} catch (IOException e) {
			logger.error("", e);
		}
		return null;
	}

	@Override
	public void remove() {
		// Old_TODO Auto-generated method stub

	}

	private Queue<FileObject> listQueue = new LinkedList<FileObject>();

	private static Logger logger = Logger.getLogger(FileIterator.class);
}
