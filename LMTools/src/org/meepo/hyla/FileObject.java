package org.meepo.hyla;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.meepo.hyla.dist.DataSegment;
import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.dist.DistributionPolicy;
import org.meepo.hyla.io.CompoundKey;
import org.meepo.hyla.io.ObjectId;
import org.meepo.hyla.io.PolymorphismWritable;
import org.meepo.hyla.io.StringWritable;
import org.meepo.hyla.storage.Storage;
import org.meepo.hyla.util.DatabaseUtils;
import org.meepo.hyla.util.FileSystemUtils;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;

public class FileObject {
	private static final Logger LOGGER = Logger.getLogger(FileObject.class);

	public static final StringWritable EMPTY_DATA = new StringWritable("");
	public static final String HANDLE_KEY_SUFFIX = " ";
	public static final String DIRECTORY_KEY_SUFFIX = "*";

	private FileSystem fileSystem;

	private FileObject parent;
	private String internalPath;
	private String internalName;
	private String path;
	private String name;

	private ObjectId id = null;
	private ObjectId handleId = null;

	public FileObject(FileSystem fileSystem, String path)
			throws IllegalArgumentException {
		this.fileSystem = fileSystem;

		String[] segs = FileSystemUtils.splitPath(path);
		if (segs.length == 0) {
			// Must be root
			this.parent = null;
			this.path = FileSystem.SEPARATOR;
			this.internalPath = FileSystem.SEPARATOR;
			this.name = null;
			this.internalName = null;
			return;
		}

		StringBuilder pathBuilder = new StringBuilder();
		for (int i = 0; i < segs.length - 1; i++) {
			pathBuilder.append(FileSystem.SEPARATOR);
			pathBuilder.append(segs[i]);
		}
		this.parent = new FileObject(fileSystem, pathBuilder.toString());
		pathBuilder.append(FileSystem.SEPARATOR);
		pathBuilder.append(segs[segs.length - 1]);
		this.path = pathBuilder.toString();
		this.internalPath = this.path.toLowerCase();
		this.name = segs[segs.length - 1];
		this.internalName = this.name.toLowerCase();
	}

	FileObject(FileSystem fileSystem, FileObject parent, String name)
			throws IllegalArgumentException {
		this(fileSystem, parent.internalPath + FileSystem.SEPARATOR + name);
	}

	/**
	 * Get parent.
	 * */
	public FileObject getParent() {
		return this.parent;
	}

	/**
	 * Get path;
	 * */
	public String getPath() {
		return this.path;
	}

	public String getRealPath() throws IllegalStateException, IOException {
		if (this.isRoot()) {
			return "";
		}

		return this.parent.getRealPath() + FileSystem.SEPARATOR
				+ this.getRealName();
	}

	public String getName() {
		return this.name;
	}

	public String getRealName() throws IllegalStateException, IOException {
		Meta meta = this.getMeta();
		if (meta == null) {
			return this.name;
		}
		return meta.getName();
	}

	/**
	 * Whether this object is root.
	 * 
	 * @return whether this object is root
	 * */
	public boolean isRoot() {
		if (this.parent == null) {
			return true;
		}
		return false;
	}

	/**
	 * Whether this object is a directory.
	 * 
	 * @return whether this object is a directory
	 * */
	public boolean isDirectory() throws IllegalStateException, IOException {
		Meta meta = this.getMeta();
		if (meta == null) {
			return false;
		}
		return meta.isDirectory();
	}

	/**
	 * Whether this object is a file.
	 * 
	 * @return whether this object is a file
	 * */
	public boolean isFile() throws IllegalStateException, IOException {
		Meta meta = this.getMeta();
		if (meta == null) {
			return false;
		}
		return meta.isFile();
	}

	/**
	 * Whether this object exists.
	 * 
	 * @return whether this object exists
	 * */
	public boolean exists() throws IllegalStateException, IOException {
		// Do a getMetadata test to see if this exists
		return this.getMeta() != null;
	}

	/**
	 * Get id of this object.
	 * 
	 * @return id of this object; null if this object doesn't exist
	 * */
	public ObjectId getId() throws IllegalStateException, IOException {
		try {
			if (this.id == null) {
				this.getIdInternal();
			}
			return this.id;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}
	}

	/**
	 * Get id of this object from database.
	 * */
	private void getIdInternal() throws IllegalStateException,
			DatabaseException, IOException {
		if (this.isRoot()) {
			this.id = FileSystem.FS_ROOT_ID;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + this.id);
			return;
		}

		ObjectId parentHandleId = this.parent.getHandleId();
		if (parentHandleId == null) {
			this.id = null;
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);

		ObjectId objId = new ObjectId();
		ret = DatabaseUtils.get(treeDb, null, new CompoundKey(parentHandleId,
				this.internalName), objId);
		if (!ret) {
			this.id = null;
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "file not exists");
			return;
		}

		this.id = objId;
		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + objId);
	}

	/**
	 * Get handle id of this object.
	 * 
	 * @return handle id of this object; null if this object doesn't exist or
	 *         it's not a directory
	 * */
	public ObjectId getHandleId() throws IllegalStateException, IOException {
		try {
			if (this.handleId == null) {
				this.getHandleIdInternal();
			}
			return this.handleId;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}
	}

	/**
	 * Get handle id of this object from database.
	 * */
	private void getHandleIdInternal() throws IllegalStateException,
			DatabaseException, IOException {
		if (this.isRoot()) {
			this.handleId = FileSystem.FS_ROOT_HANDLE_ID;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ this.handleId);
			return;
		}

		ObjectId objId = this.getId();
		if (objId == null) {
			this.handleId = null;
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "file not exists");
			return;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);

		ObjectId objHandleId = new ObjectId();
		ret = DatabaseUtils.get(treeDb, null, new CompoundKey(objId,
				DIRECTORY_KEY_SUFFIX), objHandleId);
		if (!ret) {
			this.handleId = null;
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "handle id not exists");
			return;
		}

		this.handleId = objHandleId;
		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + objHandleId);
	}

	/**
	 * Get meta data of this object.
	 * 
	 * @return meta; null if object not exists
	 * */
	public Meta getMeta() throws IllegalStateException, IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return null;
		}

		boolean ret = false;
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);

		Meta meta = new Meta();
		try {
			ret = DatabaseUtils.get(metaDb, null, objId, meta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return null;
			}
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}

		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + meta);
		return meta;
	}

	/**
	 * Update meta of this object.
	 * 
	 * @param meta
	 *            new meta of this object
	 * 
	 * @return true on success; false when object not exists
	 * */
	public boolean updateMeta(Meta meta) throws IllegalArgumentException,
			IllegalStateException, IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return false;
		}

		boolean ret = false;
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			Meta oldMeta = new Meta();
			ret = DatabaseUtils.getAndLock(metaDb, transaction, objId, oldMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return false;
			}

			if ((meta.isDirectory() != oldMeta.isDirectory())
					|| !meta.getName().equals(oldMeta.getName())) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "meta not match");
				throw new IllegalArgumentException(
						"meta not match, new meta should has the same name and type with old one");
			}

			DatabaseUtils.put(metaDb, transaction, objId, meta);

			transaction.commit();
			transaction = null;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + meta);
			return true;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * snapshot ++
	 */
	public boolean updateSnapshot(Long snapshot) throws IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return false;
		}

		boolean ret = false;
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			Meta oldMeta = new Meta();
			ret = DatabaseUtils.getAndLock(metaDb, transaction, objId, oldMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return false;
			}

			oldMeta.setSnapshot(snapshot);

			DatabaseUtils.put(metaDb, transaction, objId, oldMeta);

			transaction.commit();
			transaction = null;
			LOGGER.debug("SUCCEEDED add snapshot: '" + this.internalPath
					+ "': " + oldMeta.getSnapshot());
			return true;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	public boolean pushSnapshot() throws IOException {
		synchronized (currentGlobalSnapshot) {
			getGlobalSnapshot(this);
			currentGlobalSnapshot++;
			FileObject obj = this;
			do {
				obj.updateSnapshot(currentGlobalSnapshot);

				obj = obj.getParent();
			} while (obj != null);
			return true;
		}
	}

	public static long getGlobalSnapshot(FileObject fo)
			throws IllegalStateException, IOException {
		if (currentGlobalSnapshot > 0) {
			return currentGlobalSnapshot;
		}

		// Get root snapshot
		FileObject parent = fo.getParent();
		while (parent != null) {
			fo = parent;
			parent = fo.getParent();
		}

		currentGlobalSnapshot = fo.getMeta().getSnapshot();
		return currentGlobalSnapshot;
	}

	// public static Long pushGlobalSnapshot(FileSystem fs) {
	// synchronized (currentGlobalSnapshot) {
	// return ++currentGlobalSnapshot;
	// }
	// }

	public static Long currentGlobalSnapshot = -1L;

	/**
	 * Get extended meta data of this object.
	 * 
	 * @param extMeta
	 *            extended meta data of this object. This value is NOT defined
	 *            unless the response is a SUCCESS
	 * 
	 * @return true on success; otherwise false
	 * */
	public ExtendedMeta getExtendedMeta() throws IllegalStateException,
			IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return null;
		}

		boolean ret = false;
		Database extMetaDb = this.fileSystem
				.getDatabase(FileSystem.EXT_META_DB_NAME);

		PolymorphismWritable extData = new PolymorphismWritable();
		try {
			ret = DatabaseUtils.get(extMetaDb, null, objId, extData);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return null;
			}
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}

		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
				+ extData.getObject());
		return (ExtendedMeta) extData.getObject();
	}

	/**
	 * Put extended meta data of this object.
	 * 
	 * @param extMeta
	 *            extended meta data of this object
	 * 
	 * @return true on success; otherwise false
	 * */
	public boolean putExtendedMeta(ExtendedMeta extMeta)
			throws IllegalStateException, IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return false;
		}

		Database extMetaDb = this.fileSystem
				.getDatabase(FileSystem.EXT_META_DB_NAME);

		try {
			DatabaseUtils.put(extMetaDb, null, objId, new PolymorphismWritable(
					extMeta));
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}

		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + extMeta);
		return true;
	}

	/**
	 * Delete extended meta data of this object.
	 * 
	 * @return true on success; otherwise false
	 * */
	public boolean deleteExtendedMeta() throws IllegalStateException,
			IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return false;
		}

		Database extMetaDb = this.fileSystem
				.getDatabase(FileSystem.EXT_META_DB_NAME);

		try {
			DatabaseUtils.delete(extMetaDb, null, objId);
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}

		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "'");
		return true;
	}

	/**
	 * Get data distribution of this file.
	 * 
	 * @return distribution of this file; null when object not exists or it's
	 *         not a file
	 * */
	public Distribution getFileDistribution() throws IllegalStateException,
			IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return null;
		}

		boolean ret = false;
		Database distributionDb = this.fileSystem
				.getDatabase(FileSystem.DISTRIBUTION_DB_NAME);

		Distribution dist = new Distribution();
		try {
			ret = DatabaseUtils.get(distributionDb, null, objId, dist);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return null;
			}
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}

		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + dist);
		return dist;
	}

	public boolean updateFileDistribution(Distribution distribution)
			throws IllegalStateException, IOException {
		// Old_TODO:
		throw new RuntimeException("not implemented");
	}

	/**
	 * Get data distribution of this file.
	 * 
	 * @return statis of this object; null when object not exists or it's not a
	 *         directory or its statis is not ready
	 * */
	public Statistic getDirectoryStatis() throws IllegalStateException,
			IOException {
		ObjectId objId = this.getId();
		if (objId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "id not found");
			return null;
		}

		boolean ret = false;
		Database statisDb = this.fileSystem
				.getDatabase(FileSystem.STATIS_DB_NAME);

		Statistic statistic = new Statistic();
		try {
			ret = DatabaseUtils.get(statisDb, null, objId, statistic);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists or statis is not ready");
				return null;
			}
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}

		LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': " + statistic);
		return statistic;
	}

	/**
	 * Create this object as a file.
	 * 
	 * @param distPolicy
	 *            the distribution policy to be used for the new file
	 * 
	 * @return SUCCESS, OBJECT_ALREADY_EXISTS, UNEXPECTED_ERROR
	 * */
	public OperationResponse createFile(DistributionPolicy distPolicy)
			throws IllegalStateException, IOException {
		if (this.isRoot()) {
			throw new RuntimeException("cannot execute this operation on root");
		}

		// Do a loosely check, should filter out most cases.
		if (this.exists()) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "object already exists");
			return OperationResponse.OBJECT_ALREADY_EXISTS;
		}

		ObjectId parentHandleId = this.parent.getHandleId();
		if (parentHandleId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.PARENT_NOT_EXISTS;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Database distributionDb = this.fileSystem
				.getDatabase(FileSystem.DISTRIBUTION_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			// Lock parent and get parent meta-data
			Meta parentMeta = new Meta();
			ret = this.getAndLockParent(transaction, parentMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "parent not exists");
				return OperationResponse.PARENT_NOT_EXISTS;
			}

			// Get current time.
			long operationTime = new Date().getTime();

			// Allocate file id.
			ObjectId fileId = this.fileSystem.allocate();

			// Generate file distribution.
			Storage[] segmentStorages = distPolicy.chooseStorages(
					this.internalPath, this.fileSystem.getStorages());
			int numberOfSegments = segmentStorages.length;

			ObjectId[] segmentIds = new ObjectId[numberOfSegments];
			for (int i = 0; i < numberOfSegments; i++) {
				segmentIds[i] = this.fileSystem.allocate();
			}

			Calendar now = Calendar.getInstance();
			int year = now.get(Calendar.YEAR);
			int month = now.get(Calendar.MONTH) + 1;
			int day = now.get(Calendar.DAY_OF_MONTH);
			int hour = now.get(Calendar.HOUR_OF_DAY);
			String dirPath = year + "/" + month + "/" + day + "/" + hour + "/";
			String[] pathsOnStorage = new String[numberOfSegments];
			for (int i = 0; i < numberOfSegments; i++) {
				pathsOnStorage[i] = dirPath + segmentIds[i].toNameString();
			}

			DataSegment[] segments = new DataSegment[numberOfSegments];
			for (int i = 0; i < numberOfSegments; i++) {
				segments[i] = new DataSegment(
						this.fileSystem.getStorageId(segmentStorages[i]),
						pathsOnStorage[i]);
			}

			Distribution fileDistribution = distPolicy
					.createDistribution(segments);

			// Put file distribution
			ret = DatabaseUtils.putNoOverwrite(distributionDb, transaction,
					fileId, fileDistribution);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id already exists in distribution db: " + fileId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Put meta-data.
			Meta fileMeta = new Meta(this.name, false, operationTime);
			ret = DatabaseUtils.putNoOverwrite(metaDb, transaction, fileId,
					fileMeta);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id already exists in meta db: " + fileId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Add file to parent.
			ret = DatabaseUtils.putNoOverwrite(treeDb, transaction,
					new CompoundKey(parentHandleId, this.internalName), fileId);
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already created
				 * the same file in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object already exists");
				return OperationResponse.OBJECT_ALREADY_EXISTS;
			}

			// Unlock parent and update parent meta-data.
			parentMeta.setModifyTime(operationTime);
			parentMeta.setAccessTime(operationTime);
			this.putAndUnlockParent(transaction, parentMeta);

			// Done.
			transaction.commit();
			transaction = null;

			this.id = fileId;
			this.handleId = null;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ "created: " + fileId);
			this.pushSnapshot();
			return OperationResponse.SUCCESS;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * Delete this file.
	 * 
	 * @return SUCCESS, OBJECT_NOT_EXISTS, UNEXPECTED_ERROR
	 * */
	private OperationResponse deleteFile() throws IllegalStateException,
			IOException {
		// Do a loosely check, should filter out most cases.
		if (!this.exists()) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "object not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		ObjectId parentHandleId = this.parent.getHandleId();
		if (parentHandleId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Database extMetaDb = this.fileSystem
				.getDatabase(FileSystem.EXT_META_DB_NAME);
		Database distributionDb = this.fileSystem
				.getDatabase(FileSystem.DISTRIBUTION_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			// Lock parent and get parent meta-data.
			Meta parentMeta = new Meta();
			ret = this.getAndLockParent(transaction, parentMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "parent not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Get current time.
			long operationTime = new Date().getTime();

			// Remove file from parent.
			ObjectId fileId = new ObjectId();
			ret = DatabaseUtils.getAndDelete(treeDb, transaction,
					new CompoundKey(parentHandleId, this.internalName), fileId);
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already deleted
				 * the file in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Delete meta-data.
			ret = DatabaseUtils.delete(metaDb, transaction, fileId);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id not exists in meta db: " + fileId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Delete extended meta-data, never mind if this fails since
			// extended meta-data may not exists for this file.
			DatabaseUtils.delete(extMetaDb, transaction, fileId);

			// Delete file distribution.
			ret = DatabaseUtils.delete(distributionDb, transaction, fileId);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id not exists in distribution db: " + fileId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Unlock parent and update parent meta-data.
			parentMeta.setModifyTime(operationTime);
			parentMeta.setAccessTime(operationTime);
			this.putAndUnlockParent(transaction, parentMeta);

			// Done.
			transaction.commit();
			transaction = null;

			this.id = null;
			this.handleId = null;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ "deleted: " + fileId);
			return OperationResponse.SUCCESS;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * Create this object as a directory.
	 * 
	 * @return SUCCESS, OBJECT_ALREADY_EXISTS, UNEXPECTED_ERROR
	 * */
	public OperationResponse makeDirectory() throws IllegalStateException,
			IOException {
		if (this.isRoot()) {
			throw new RuntimeException("cannot operate root");
		}

		// Do a loosely check, should filter out most cases.
		if (this.exists()) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "object already exists");
			return OperationResponse.OBJECT_ALREADY_EXISTS;
		}

		ObjectId parentHandleId = this.parent.getHandleId();
		if (parentHandleId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.PARENT_NOT_EXISTS;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			// Lock parent and get parent meta-data.
			Meta parentMeta = new Meta();
			ret = this.getAndLockParent(transaction, parentMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "parent not exists");
				return OperationResponse.PARENT_NOT_EXISTS;
			}

			// Get current time.
			long operationTime = new Date().getTime();

			// Allocate directory id and directory handle id.
			ObjectId dirId = this.fileSystem.allocate();
			ObjectId dirHandleId = this.fileSystem.allocate();

			// Put directory handle id.
			ret = DatabaseUtils
					.putNoOverwrite(treeDb, transaction, new CompoundKey(
							dirHandleId, HANDLE_KEY_SUFFIX), EMPTY_DATA);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "handle id already exists in tree db: " + dirHandleId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Put directory id.
			ret = DatabaseUtils.putNoOverwrite(treeDb, transaction,
					new CompoundKey(dirId, DIRECTORY_KEY_SUFFIX), dirHandleId);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id already exists in tree db: " + dirId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Put meta-data.
			Meta dirMeta = new Meta(this.name, true, operationTime);
			ret = DatabaseUtils.putNoOverwrite(metaDb, transaction, dirId,
					dirMeta);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id already exists in meta db: " + dirId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Add directory to parent.
			ret = DatabaseUtils.putNoOverwrite(treeDb, transaction,
					new CompoundKey(parentHandleId, this.internalName), dirId);
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already create
				 * the same directory in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object already exists");
				return OperationResponse.OBJECT_ALREADY_EXISTS;
			}

			// Unlock parent and update parent meta-data.
			parentMeta.setModifyTime(operationTime);
			parentMeta.setAccessTime(operationTime);
			this.putAndUnlockParent(transaction, parentMeta);

			// Done.
			transaction.commit();
			transaction = null;

			this.id = dirId;
			this.handleId = dirHandleId;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ "created: " + dirId + " " + dirHandleId);
			this.pushSnapshot();
			return OperationResponse.SUCCESS;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * Make all non-existing directories along the path of this object. NOT
	 * recommended!
	 * 
	 * @return SUCCESS, OBJECT_ALREADY_EXISTS, UNEXPECTED_ERROR
	 * */
	public OperationResponse makeDirectories() throws IllegalStateException,
			IOException {
		if (this.isRoot()) {
			throw new RuntimeException("cannot execute this operation on root");
		}

		if (!this.parent.isRoot()) {
			this.parent.makeDirectories();
		}
		return this.makeDirectory();
	}

	/**
	 * Delete this directory.
	 * 
	 * @return SUCCESS, OBJECT_NOT_EXISTS, UNEXPECTED_ERROR
	 * */
	private OperationResponse deleteDirectory() throws IllegalStateException,
			IOException {
		// Do a loosely check, should filter out most cases.
		if (!this.exists()) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "object not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		ObjectId parentHandleId = this.parent.getHandleId();
		if (parentHandleId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Database extMetaDb = this.fileSystem
				.getDatabase(FileSystem.EXT_META_DB_NAME);
		Transaction transaction = null;
		Cursor cursor = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			// Lock parent and get parent meta-data.
			Meta parentMeta = new Meta();
			ret = this.getAndLockParent(transaction, parentMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "parent not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Get current time.
			long operationTime = new Date().getTime();

			// Remove directory from parent.
			ObjectId dirId = new ObjectId();
			ret = DatabaseUtils.getAndDelete(treeDb, transaction,
					new CompoundKey(parentHandleId, this.internalName), dirId);
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already deleted
				 * the directory in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Delete meta-data.
			ret = DatabaseUtils.delete(metaDb, transaction, dirId);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id not exists in meta db: " + dirId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Delete extended meta-data, never mind if this fails since
			// extended meta-data may not exists for this file.
			DatabaseUtils.delete(extMetaDb, transaction, dirId);

			// Delete directory id.
			ObjectId dirHandleId = new ObjectId();
			ret = DatabaseUtils.getAndDelete(treeDb, transaction,
					new CompoundKey(dirId, DIRECTORY_KEY_SUFFIX), dirHandleId);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id not exists in tree db: " + dirId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			cursor = this.fileSystem.openDatabaseCursor(treeDb, transaction);

			// Delete directory handle id.
			ret = DatabaseUtils.getSearchKeyAndDelete(treeDb, cursor,
					new CompoundKey(dirHandleId, HANDLE_KEY_SUFFIX), null);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "handle id not exists in tree db: " + dirId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Delete objects under directory.
			LinkedList<ObjectId> objectIdList = new LinkedList<ObjectId>();
			CompoundKey entryKey = new CompoundKey(new ObjectId(), null);
			ObjectId objectId = new ObjectId();
			while (true) {
				ret = DatabaseUtils.getNextMatchAndDelete(treeDb, cursor,
						dirHandleId, entryKey, objectId);
				if (!ret) {
					break;
				}
				objectIdList.add(objectId.clone());
			}
			cursor.close();
			cursor = null;

			// Unlock parent and update parent meta-data.
			parentMeta.setModifyTime(operationTime);
			parentMeta.setAccessTime(operationTime);
			this.putAndUnlockParent(transaction, parentMeta);

			// Done.
			transaction.commit();
			transaction = null;

			// Mark all objects under directory as garbages to be recycled.
			if (objectIdList.size() != 0) {
				this.fileSystem.addGarbages(objectIdList);
			}

			this.id = null;
			this.handleId = null;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ "deleted: " + dirId + " " + dirHandleId);
			return OperationResponse.SUCCESS;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * Delete this object.
	 * 
	 * @return SUCCESS, OBJECT_NOT_EXISTS, UNEXPECTED_ERROR
	 * */
	public OperationResponse delete() throws IllegalStateException, IOException {
		if (this.isRoot()) {
			throw new RuntimeException("cannot execute this operation on root");
		}

		boolean isDirectory = this.isDirectory();

		if (isDirectory) {
			return this.deleteDirectory();
		} else {
			return this.deleteFile();
		}
	}

	/**
	 * Rename this object to another name.
	 * 
	 * @param dstObj
	 *            destination to be renamed to of this object
	 * 
	 * @return SUCCESS, OBJECT_NOT_EXISTS, OBJECT_ALREADY_EXISTS,
	 *         UNEXPECTED_ERROR
	 * */
	private OperationResponse renameTo(FileObject dstObj)
			throws IllegalStateException, IOException {
		// Do a loosely check on source.
		if (!this.exists()) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "object not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		boolean isDummyRename = false;
		if (this.internalName.equals(dstObj.internalName)) {
			isDummyRename = true;
		}

		// Do a loosely check on destination.
		if (!isDummyRename && dstObj.exists()) {
			LOGGER.debug("FAILED: '" + dstObj.internalPath + "': "
					+ "object already exists");
			return OperationResponse.OBJECT_ALREADY_EXISTS;
		}

		ObjectId parentHandleId = this.parent.getHandleId();
		if (parentHandleId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			// Lock parent and get parent meta-data.
			Meta parentMeta = new Meta();
			ret = this.getAndLockParent(transaction, parentMeta);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "parent not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Get current time
			long operationTime = new Date().getTime();

			ObjectId objId = new ObjectId();
			if (!isDummyRename) {
				// Remove source object from parent.
				ret = DatabaseUtils.getAndDelete(treeDb, transaction,
						new CompoundKey(parentHandleId, this.internalName),
						objId);
			} else {
				// Get object id.
				ret = DatabaseUtils.get(treeDb, transaction, new CompoundKey(
						parentHandleId, this.internalName), objId);
			}
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already deleted
				 * source file in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Get meta-data.
			Meta objMeta = new Meta();
			ret = DatabaseUtils.get(metaDb, transaction, objId, objMeta);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id not exists in meta db: " + objId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Update meta-data.
			objMeta.setName(dstObj.name);
			objMeta.setAccessTime(operationTime);

			// Put meta-data, no need to check this since put() will never
			// return a failure.
			DatabaseUtils.put(metaDb, transaction, objId, objMeta);

			if (!isDummyRename) {
				// Add destination object to parent.
				ret = DatabaseUtils.putNoOverwrite(treeDb, transaction,
						new CompoundKey(parentHandleId, dstObj.internalName),
						objId);
				if (!ret) {
					/*
					 * It's OK for the above operation to fail, since we only
					 * did a loosely check at the beginning. Someone may already
					 * created destination file in the gap between check and
					 * lock.
					 */
					LOGGER.debug("FAILED: '" + this.internalPath + "': "
							+ "object not exists");
					return OperationResponse.OBJECT_NOT_EXISTS;
				}
			}

			// Unlock parent and update parent meta-data.
			parentMeta.setModifyTime(operationTime);
			parentMeta.setAccessTime(operationTime);
			this.putAndUnlockParent(transaction, parentMeta);

			// Done.
			transaction.commit();
			transaction = null;

			this.id = objId;
			this.path = dstObj.path;
			this.internalPath = dstObj.internalPath;
			this.name = dstObj.name;
			this.internalName = dstObj.internalName;
			dstObj.id = objId;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ "renamed to: '" + dstObj.internalPath + "'");
			return OperationResponse.SUCCESS;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * Move this object to be another object.
	 * 
	 * @param dstObj
	 *            destination to be moved to of this object
	 * 
	 * @return SUCCESS, OBJECT_NOT_EXISTS, OBJECT_ALREADY_EXISTS,
	 *         PARENT_NOT_EXISTS, UNEXPECTED_ERROR
	 * */
	public OperationResponse moveTo(FileObject dstObj)
			throws IllegalStateException, IOException {
		if (dstObj.fileSystem != this.fileSystem) {
			throw new RuntimeException("cannot move between two file systems");
		}

		if (this.isRoot() || dstObj.isRoot()) {
			throw new RuntimeException("cannot execute this operation on root");
		}

		if (this.parent.internalPath
				.equalsIgnoreCase(dstObj.parent.internalPath)) {
			return this.renameTo(dstObj);
		}

		// You cannot move a directory as a sub-directory of its own
		// Fixed by ms.
		if (this.internalPath.equalsIgnoreCase(dstObj.parent.internalPath)) {
			return OperationResponse.PARENT_NOT_EXISTS;
		}

		// Do a loosely check on source.
		if (!this.exists()) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "object not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		// Do a loosely check on destination.
		if (dstObj.exists()) {
			LOGGER.debug("FAILED: '" + dstObj.internalPath + "': "
					+ "object already exists");
			return OperationResponse.OBJECT_ALREADY_EXISTS;
		}

		ObjectId srcParentHandleId = this.parent.getHandleId();
		if (srcParentHandleId == null) {
			LOGGER.debug("FAILED: '" + this.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.OBJECT_NOT_EXISTS;
		}

		ObjectId dstParentHandleId = dstObj.parent.getHandleId();
		if (dstParentHandleId == null) {
			LOGGER.debug("FAILED: '" + dstObj.internalPath + "': "
					+ "parent not exists");
			return OperationResponse.PARENT_NOT_EXISTS;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.fileSystem.beginDatabaseTransaction();

			// Lock parent and get parent meta-data.
			Meta srcParentMeta = new Meta();
			Meta dstParentMeta = new Meta();
			if (srcParentHandleId.compareTo(dstParentHandleId) < 0) {
				ret = this.getAndLockParent(transaction, srcParentMeta);
				if (!ret) {
					LOGGER.debug("FAILED: '" + this.internalPath + "': "
							+ "source parent not exists");
					return OperationResponse.OBJECT_NOT_EXISTS;
				}

				ret = dstObj.getAndLockParent(transaction, dstParentMeta);
				if (!ret) {
					LOGGER.debug("FAILED: '" + this.internalPath + "': "
							+ "destination parent not exists");
					return OperationResponse.PARENT_NOT_EXISTS;
				}
			} else {
				ret = dstObj.getAndLockParent(transaction, dstParentMeta);
				if (!ret) {
					LOGGER.debug("FAILED: '" + this.internalPath + "': "
							+ "destination parent not exists");
					return OperationResponse.PARENT_NOT_EXISTS;
				}

				ret = this.getAndLockParent(transaction, srcParentMeta);
				if (!ret) {
					LOGGER.debug("FAILED: '" + this.internalPath + "': "
							+ "source parent not exists");
					return OperationResponse.OBJECT_NOT_EXISTS;
				}
			}

			// Get current time.
			long operationTime = new Date().getTime();

			// Remove source object from parent.
			ObjectId objId = new ObjectId();
			ret = DatabaseUtils.getAndDelete(treeDb, transaction,
					new CompoundKey(srcParentHandleId, this.internalName),
					objId);
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already deleted
				 * the object in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + this.internalPath + "': "
						+ "object not exists");
				return OperationResponse.OBJECT_NOT_EXISTS;
			}

			// Get meta-data.
			Meta objMeta = new Meta();
			ret = DatabaseUtils.get(metaDb, transaction, objId, objMeta);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: '" + this.internalPath + "': "
						+ "id not exists in meta db: " + objId);
				return OperationResponse.UNEXPECTED_ERROR;
			}

			// Update meta-data.
			objMeta.setName(dstObj.name);
			objMeta.setAccessTime(operationTime);

			// Put meta-data, no need to check this since put() will never
			// return a failure.
			DatabaseUtils.put(metaDb, transaction, objId, objMeta);

			// Add object to parent.
			ret = DatabaseUtils.putNoOverwrite(treeDb, transaction,
					new CompoundKey(dstParentHandleId, dstObj.internalName),
					objId);
			if (!ret) {
				/*
				 * It's OK for the above operation to fail, since we only did a
				 * loosely check at the beginning. Someone may already created
				 * the same object in the gap between check and lock.
				 */
				LOGGER.debug("FAILED: '" + dstObj.internalPath + "': "
						+ "object already exists");
				return OperationResponse.OBJECT_ALREADY_EXISTS;
			}

			// Unlock parent and update parent meta-data.
			srcParentMeta.setModifyTime(operationTime);
			srcParentMeta.setAccessTime(operationTime);
			this.putAndUnlockParent(transaction, srcParentMeta);
			dstParentMeta.setModifyTime(operationTime);
			dstParentMeta.setAccessTime(operationTime);
			dstObj.putAndUnlockParent(transaction, dstParentMeta);

			transaction.commit();
			transaction = null;

			this.id = objId;
			this.path = dstObj.path;
			this.internalPath = dstObj.internalPath;
			this.name = dstObj.name;
			this.internalName = dstObj.internalName;
			dstObj.id = objId;
			LOGGER.debug("SUCCEEDED: '" + this.internalPath + "': "
					+ "renamed to: '" + dstObj.internalPath + "'");
			return OperationResponse.SUCCESS;
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	/**
	 * List contents of this directory.
	 * 
	 * @return An array of objects under this directory; null if this directory
	 *         doesn't exists. An empty array will be returned if this directory
	 *         contains no object.
	 * */
	public FileObject[] list() throws IllegalStateException, IOException {
		ObjectId objHandleId = this.getHandleId();
		if (objHandleId == null) {
			return null;
		}

		boolean ret = false;
		Database treeDb = this.fileSystem.getDatabase(FileSystem.TREE_DB_NAME);
		Cursor cursor = null;

		try {
			cursor = this.fileSystem.openDatabaseCursor(treeDb, null);

			ret = DatabaseUtils.getSearchKey(treeDb, cursor, new CompoundKey(
					objHandleId, HANDLE_KEY_SUFFIX), null);
			if (!ret) {
				LOGGER.debug("FAILED: '" + this.internalPath
						+ "': handle id not found");
				return null;
			}

			LinkedList<FileObject> objectList = new LinkedList<FileObject>();
			CompoundKey entryKey = new CompoundKey(new ObjectId(), null);
			ObjectId objectId = new ObjectId();
			String objName;
			FileObject obj;
			while (true) {
				ret = DatabaseUtils.getNextMatch(treeDb, cursor, objHandleId,
						entryKey, objectId);
				if (!ret) {
					break;
				}

				objName = entryKey.getSuffix();
				obj = new FileObject(this.fileSystem, this, objName);
				obj.id = objectId.clone();
				objectList.add(obj);
			}

			cursor.close();
			cursor = null;
			LOGGER.debug("SUCCESS: '" + this.internalPath + "': "
					+ objectList.size());
			return objectList.toArray(new FileObject[0]);
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	/**
	 * Get attributes of this object.
	 * 
	 * @return An instance of Meta contains this object's attributes; null if
	 *         this object doesn't exist
	 * */
	public Meta getAttributes() throws IllegalStateException, IOException {
		return this.getMeta();
	}

	@Override
	public String toString() {
		return "FileObject {realPath:" + this.internalPath + "}";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof FileObject)) {
			return false;
		}
		FileObject that = (FileObject) obj;
		return this.internalPath.equals(that.internalPath);
	}

	@Override
	public int hashCode() {
		return this.internalPath.hashCode();
	}

	/**
	 * Get meta data of parent and put a write lock on it.
	 * 
	 * @param transaction
	 *            the transaction to be used for this operation
	 * @param parentMeta
	 *            meta data of parent
	 * 
	 * @return true on success; otherwise false
	 * */
	private boolean getAndLockParent(Transaction transaction, Meta parentMeta)
			throws IllegalStateException, DatabaseException, IOException {
		if (this.isRoot()) {
			return false;
		}

		ObjectId parentId = this.parent.getId();
		if (parentId == null) {
			return false;
		}

		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);

		return DatabaseUtils.getAndLock(metaDb, transaction, parentId,
				parentMeta);
	}

	/**
	 * Put meta data of parent and release the write lock.
	 * 
	 * @param transaction
	 *            the transaction to be used for this operation
	 * @param parentMeta
	 *            meta data of parent
	 * */
	private void putAndUnlockParent(Transaction transaction, Meta parentMeta)
			throws IllegalStateException, DatabaseException, IOException {
		if (this.isRoot()) {
			return;
		}

		ObjectId parentId = this.parent.getId();
		if (parentId == null) {
			return;
		}

		Database metaDb = this.fileSystem.getDatabase(FileSystem.META_DB_NAME);

		// The lock will be released with the put operation.
		DatabaseUtils.put(metaDb, transaction, parentId, parentMeta);
	}
}
