package org.meepo.hyla;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.meepo.firewall.CipherManager;
import org.meepo.hyla.io.CompoundKey;
import org.meepo.hyla.io.IntegerWritable;
import org.meepo.hyla.io.ObjectId;
import org.meepo.hyla.io.PolymorphismWritable;
import org.meepo.hyla.io.StringWritable;
import org.meepo.hyla.storage.Storage;
import org.meepo.hyla.util.DatabaseUtils;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class FileSystem {
	public static final int MAJOR_VERSION = 0;
	public static final int MINOR_VERSION = 1;
	public static final String VERSION_STRING = "Hyla v0.01";

	private static final Logger LOGGER = Logger.getLogger(FileSystem.class);

	public static final String SEPARATOR = "/";
	public static final char SEPARATOR_CHAR = '/';
	public static final CharSequence[] ILLEGAL_CHAR_SEQUENCES = { "\\", "/",
			":", "?", "<", ">", "|", "*", "\"" };
	public static final String ILLEGAL_CHARS = "\\/:?<>|*\"";

	public static final int MAX_SEGMENT_LENGTH = 256;
	public static final int MAX_NUMBER_OF_SEGMENTS = 256;

	public static final short RESERVED_DOMAIN = 0;
	public static final short DEFAULT_DOMAIN = 1;
	public static final ObjectId FS_ROOT_ID = new ObjectId(RESERVED_DOMAIN, 0,
			0x1);
	public static final ObjectId FS_ROOT_HANDLE_ID = new ObjectId(
			RESERVED_DOMAIN, 0, 0x2);

	protected static final String HEADER_DB_NAME = "HeaderDb";
	protected static final String TREE_DB_NAME = "TreeDb";
	protected static final String META_DB_NAME = "MetaDb";
	protected static final String EXT_META_DB_NAME = "ExtMetaDb";
	protected static final String DISTRIBUTION_DB_NAME = "DistributionDb";
	protected static final String STORAGE_DB_NAME = "StorageDb";
	protected static final String STATIS_DB_NAME = "StatisDb";

	private AtomicBoolean shouldStop = new AtomicBoolean(false);

	private File home;
	private boolean allowCreate;
	private short domain;

	private DatabaseManager databaseManager = null;
	private IdAllocator idAllocator = null;
	private StorageManager storageManager = null;
	private GarbageCollector garbageCollector = null;
	private StatisSummer statisSummer = null;
	private TrashCleaner trashCleaner = null;
	private CipherManager cipherManager = null;

	public FileSystem(File home, boolean allowCreate, short domain)
			throws IllegalArgumentException, IOException {
		if (domain == RESERVED_DOMAIN) {
			throw new IllegalArgumentException("domain " + RESERVED_DOMAIN
					+ " is reserved for special use");
		}

		this.home = home;
		this.allowCreate = allowCreate;
		this.domain = domain;

		try {
			this.initialize();
		} catch (DatabaseException e) {
			this.close();
			throw new IOException(e);
		}
	}

	public FileSystem(File home, boolean allowCreate) throws IOException {
		this(home, allowCreate, DEFAULT_DOMAIN);
	}

	private void initialize() throws DatabaseException, IOException {
		LOGGER.info("initializing file system");

		this.databaseManager = new DatabaseManager();
		this.idAllocator = new IdAllocator();
		this.storageManager = new StorageManager();

		this.garbageCollector = new GarbageCollector();
		this.garbageCollector.start();
		this.statisSummer = new StatisSummer();
		this.statisSummer.start();
		// start schedule job
		// disabled TrashCleaner and cipherManager
		// this.trashCleaner = TrashCleaner.getInstance();
		// this.trashCleaner.scheduleMJob();

		// this.cipherManager = CipherManager.getInstance();
		// this.cipherManager.scheduleMJob();

		this.initializeRoot();

		LOGGER.info("initialized file system");
	}

	private void initializeRoot() throws DatabaseException, IOException {
		if (new FileObject(this, "/").exists()) {
			LOGGER.info("initialized file system");
			return;
		}

		if (!this.allowCreate) {
			LOGGER.error("root not found and not allow create");
			throw new IOException("root not found and not allow create");
		}

		boolean ret = false;
		Database treeDb = this.databaseManager
				.getDatabase(FileSystem.TREE_DB_NAME);
		Database metaDb = this.databaseManager
				.getDatabase(FileSystem.META_DB_NAME);
		Transaction transaction = null;

		try {
			transaction = this.databaseManager.beginTransaction();

			// Get current time.
			long operationTime = new Date().getTime();

			// Put root handle id.
			ret = DatabaseUtils.putNoOverwrite(treeDb, transaction,
					new CompoundKey(FS_ROOT_HANDLE_ID,
							FileObject.HANDLE_KEY_SUFFIX),
					FileObject.EMPTY_DATA);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: root handle id already exists in tree db");
				throw new IOException(
						"root handle id already exists in tree db");
			}

			// Put directory id.
			ret = DatabaseUtils
					.putNoOverwrite(treeDb, transaction, new CompoundKey(
							FS_ROOT_ID, FileObject.DIRECTORY_KEY_SUFFIX),
							FS_ROOT_HANDLE_ID);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: root id already exists in tree db");
				throw new IOException("root id already exists in tree db");
			}

			// Put meta-data.
			Meta rootMeta = new Meta("", true, operationTime);
			ret = DatabaseUtils.putNoOverwrite(metaDb, transaction, FS_ROOT_ID,
					rootMeta);
			if (!ret) {
				// Unexpected, log it and abort.
				LOGGER.error("UNEXPECTED: root id already exists in meta db");
				throw new IOException("root id already exists in meta db");
			}

			transaction.commit();
			transaction = null;
		} finally {
			if (transaction != null) {
				transaction.abort();
			}
		}
	}

	public void close() {
		if (this.shouldStop.getAndSet(true)) {
			return;
		}

		LOGGER.info("closing file system");

		if (this.statisSummer != null) {
			this.statisSummer.close();
			this.statisSummer = null;
		}
		if (this.garbageCollector != null) {
			this.garbageCollector.close();
			this.garbageCollector = null;
		}

		try {
			if (this.databaseManager != null) {
				this.databaseManager.close();
				this.databaseManager = null;
			}
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
		}

		LOGGER.info("closed file system");
	}

	private void checkRunning() throws IllegalStateException {
		if (this.shouldStop.get()) {
			throw new IllegalStateException("file system is not running");
		}
	}

	public FileObject openObject(String path) throws IllegalArgumentException {
		return new FileObject(this, path);
	}

	private class DatabaseManager {
		private final Logger LOGGER = Logger.getLogger(DatabaseManager.class);

		private TransactionConfig transactionConfig;
		private CursorConfig cursorConfig;

		private Environment dbEnv = null;
		private Database headerDb = null;
		private Database treeDb = null;
		private Database metaDb = null;
		private Database extMetaDb = null;
		private Database distributionDb = null;
		private Database storageDb = null;
		private Database statisDb = null;

		public DatabaseManager() throws DatabaseException {
			this.transactionConfig = new TransactionConfig();
			this.transactionConfig.setReadUncommitted(true);
			this.cursorConfig = new CursorConfig();
			this.cursorConfig.setReadUncommitted(true);

			this.initialize();
		}

		private void initialize() throws DatabaseException {
			LOGGER.info("initializing database manager");

			try {
				EnvironmentConfig dbEnvConfig = null;
				dbEnvConfig = new EnvironmentConfig();
				dbEnvConfig.setAllowCreate(allowCreate);
				dbEnvConfig.setTransactional(true);

				this.dbEnv = new Environment(home, dbEnvConfig);

				DatabaseConfig dbConfig = null;
				dbConfig = new DatabaseConfig();
				dbConfig.setAllowCreate(allowCreate);
				dbConfig.setTransactional(true);
				dbConfig.setSortedDuplicates(false);
				dbConfig.setCacheMode(CacheMode.KEEP_HOT);

				this.headerDb = this.dbEnv.openDatabase(null, HEADER_DB_NAME,
						dbConfig);
				this.treeDb = this.dbEnv.openDatabase(null, TREE_DB_NAME,
						dbConfig);
				this.metaDb = this.dbEnv.openDatabase(null, META_DB_NAME,
						dbConfig);
				this.extMetaDb = this.dbEnv.openDatabase(null,
						EXT_META_DB_NAME, dbConfig);
				this.distributionDb = this.dbEnv.openDatabase(null,
						DISTRIBUTION_DB_NAME, dbConfig);
				this.storageDb = this.dbEnv.openDatabase(null, STORAGE_DB_NAME,
						dbConfig);

				DatabaseConfig statisDbConfig = null;
				statisDbConfig = new DatabaseConfig();
				statisDbConfig.setAllowCreate(true);
				statisDbConfig.setTransactional(true);
				statisDbConfig.setSortedDuplicates(false);
				// statisDbConfig.setTemporary(true);
				this.statisDb = this.dbEnv.openDatabase(null, STATIS_DB_NAME,
						statisDbConfig);

				LOGGER.info("initialized database manager");
			} catch (DatabaseException e) {
				this.close();
				throw e;
			}
		}

		public void close() throws DatabaseException {
			LOGGER.info("closing database manager");

			if (this.headerDb != null) {
				this.headerDb.close();
				this.headerDb = null;
			}
			if (this.treeDb != null) {
				this.treeDb.close();
				this.treeDb = null;
			}
			if (this.metaDb != null) {
				this.metaDb.close();
				this.metaDb = null;
			}
			if (this.extMetaDb != null) {
				this.extMetaDb.close();
				this.extMetaDb = null;
			}
			if (this.distributionDb != null) {
				this.distributionDb.close();
				this.distributionDb = null;
			}
			if (this.storageDb != null) {
				this.storageDb.close();
				this.storageDb = null;
			}
			if (this.statisDb != null) {
				this.statisDb.close();
				this.statisDb = null;
			}

			if (this.dbEnv != null) {
				this.dbEnv.close();
				this.dbEnv = null;
			}

			LOGGER.info("closed database manager");
		}

		public Database getDatabase(String dbName) {
			if (dbName.equals(HEADER_DB_NAME)) {
				return this.headerDb;
			} else if (dbName.equals(TREE_DB_NAME)) {
				return this.treeDb;
			} else if (dbName.equals(META_DB_NAME)) {
				return this.metaDb;
			} else if (dbName.equals(EXT_META_DB_NAME)) {
				return this.extMetaDb;
			} else if (dbName.equals(DISTRIBUTION_DB_NAME)) {
				return this.distributionDb;
			} else if (dbName.equals(STORAGE_DB_NAME)) {
				return this.storageDb;
			} else if (dbName.equals(STATIS_DB_NAME)) {
				return this.statisDb;
			} else {
				return null;
			}
		}

		public Transaction beginTransaction() throws DatabaseException {
			return this.dbEnv.beginTransaction(null, this.transactionConfig);
		}

		public Cursor openCursor(Database database, Transaction transaction)
				throws DatabaseException {
			return database.openCursor(transaction, this.cursorConfig);
		}
	}

	Database getDatabase(String dbName) throws IllegalStateException {
		this.checkRunning();
		return this.databaseManager.getDatabase(dbName);
	}

	Transaction beginDatabaseTransaction() throws IllegalStateException,
			DatabaseException {
		this.checkRunning();
		return this.databaseManager.beginTransaction();
	}

	Cursor openDatabaseCursor(Database database, Transaction transaction)
			throws IllegalStateException, DatabaseException {
		this.checkRunning();
		return this.databaseManager.openCursor(database, transaction);
	}

	private class StorageManager {
		private final Logger LOGGER = Logger.getLogger(StorageManager.class);

		private CopyOnWriteArrayList<Storage> storageList = new CopyOnWriteArrayList<Storage>();
		private Hashtable<ObjectId, Storage> storageMap = new Hashtable<ObjectId, Storage>();

		public StorageManager() throws DatabaseException, IOException {
			this.initialize();
		}

		private void initialize() throws DatabaseException, IOException {
			LOGGER.info("initializing storage manager");

			boolean ret = false;
			Database storageDatabase = databaseManager
					.getDatabase(STORAGE_DB_NAME);
			Cursor cursor = null;

			try {
				cursor = databaseManager.openCursor(storageDatabase, null);

				LinkedList<Storage> tmpStorageList = new LinkedList<Storage>();
				while (true) {
					PolymorphismWritable storageKey = new PolymorphismWritable();
					ObjectId storageId = new ObjectId();
					ret = DatabaseUtils.getNext(storageDatabase, cursor,
							storageKey, storageId);
					if (!ret) {
						break;
					}

					this.storageMap.put(storageId,
							(Storage) storageKey.getObject());
					tmpStorageList.add((Storage) storageKey.getObject());
					LOGGER.info(storageId + " "
							+ (Storage) storageKey.getObject());
				}

				this.storageList.addAll(tmpStorageList);
				LOGGER.info("initialized storage manager, "
						+ this.storageMap.size() + " storage found in database");
			} finally {
				if (cursor != null) {
					cursor.close();
				}
			}
		}

		public boolean addStorage(Storage storage) throws DatabaseException,
				IOException {
			boolean ret = false;
			Database storageDatabase = databaseManager
					.getDatabase(STORAGE_DB_NAME);

			PolymorphismWritable storageKey = new PolymorphismWritable(storage);
			ObjectId storageId = idAllocator.allocate();

			ret = DatabaseUtils.putNoOverwrite(storageDatabase, null,
					storageKey, storageId);
			if (!ret) {
				LOGGER.debug("FAILED: " + storage + " duplicates in database");
				return false;
			}

			this.storageMap.put(storageId, storage);
			this.storageList.add(storage);
			LOGGER.debug("SUCCEEDED: " + storage + " added");
			return true;
		}

		public int addStorages(Collection<Storage> storages)
				throws DatabaseException, IOException {
			boolean ret = false;
			Database storageDatabase = databaseManager
					.getDatabase(STORAGE_DB_NAME);

			int count = 0;
			LinkedList<Storage> tmpStorageList = new LinkedList<Storage>();
			PolymorphismWritable storageKey;
			ObjectId storageId;
			for (Storage storage : storages) {
				storageKey = new PolymorphismWritable(storage);
				storageId = idAllocator.allocate();
				ret = DatabaseUtils.putNoOverwrite(storageDatabase, null,
						storageKey, storageId);
				if (!ret) {
					LOGGER.debug("FAILED: " + storage
							+ " duplicates in database, " + count
							+ " storages added");
					return count;
				}

				this.storageMap.put(storageId, storage);
				tmpStorageList.add(storage);
			}

			this.storageList.addAll(tmpStorageList);
			LOGGER.debug("SUCCEEDED: " + count + " storages added");
			return count;

		}

		public Storage getStorage(ObjectId storageId) {
			return this.storageMap.get(storageId);
		}

		public Storage[] getStorages() {
			return this.storageList.toArray(new Storage[0]);
		}

		public ObjectId getStorageId(Storage storage) throws DatabaseException,
				IOException {
			boolean ret = false;
			Database storageDatabase = databaseManager
					.getDatabase(STORAGE_DB_NAME);

			PolymorphismWritable storageKey = new PolymorphismWritable(storage);
			ObjectId storageId = new ObjectId();

			ret = DatabaseUtils.get(storageDatabase, null, storageKey,
					storageId);
			if (!ret) {
				LOGGER.debug("FAILED: " + storage + " : id not found");
				return null;
			}

			LOGGER.debug("SUCCEEDED: " + storage + " : " + storageId);
			return storageId;
		}
	}

	public Storage getStorage(ObjectId storageId) throws IllegalStateException {
		this.checkRunning();
		return this.storageManager.getStorage(storageId);
	}

	public Storage[] getStorages() throws IllegalStateException {
		this.checkRunning();
		return this.storageManager.getStorages();
	}

	public ObjectId getStorageId(Storage storage) throws IllegalStateException,
			IOException {
		this.checkRunning();
		try {
			return this.storageManager.getStorageId(storage);
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}
	}

	public boolean addStorage(Storage storage) throws IllegalStateException,
			IOException {
		this.checkRunning();
		try {
			return this.storageManager.addStorage(storage);
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}
	}

	public int addStorages(Collection<Storage> storages)
			throws IllegalStateException, IOException {
		this.checkRunning();
		try {
			return this.storageManager.addStorages(storages);
		} catch (DatabaseException e) {
			LOGGER.error("database exception", e);
			throw new IOException(e);
		}
	}

	private class IdAllocator {
		private final Logger LOGGER = Logger.getLogger(IdAllocator.class);

		private static final String KEY_STRING = "Generation";

		private int currentGeneration;
		private AtomicLong currentIdentity = null;

		public IdAllocator() throws IllegalArgumentException,
				DatabaseException, IOException {
			this.initialize();
		}

		private void initialize() throws DatabaseException, IOException {
			LOGGER.info("initializing id allocator");

			boolean ret = false;
			Database headerDb = databaseManager.getDatabase(HEADER_DB_NAME);
			Transaction transaction = null;

			try {
				transaction = databaseManager.beginTransaction();

				StringWritable keyStr = new StringWritable(KEY_STRING);
				IntegerWritable generation = new IntegerWritable();
				ret = DatabaseUtils.getAndDelete(headerDb, transaction, keyStr,
						generation);
				if (ret) {
					this.currentGeneration = generation.getValue() + 1;
				} else if (allowCreate) {
					this.currentGeneration = 1;
				} else {
					LOGGER.error("id generation not found and not allow create");
					throw new RuntimeException(
							"id generation not found and not allow create");
				}

				IntegerWritable newGeneration = new IntegerWritable(
						this.currentGeneration);
				DatabaseUtils.put(headerDb, transaction, keyStr, newGeneration);

				transaction.commit();
				transaction = null;

				this.currentIdentity = new AtomicLong(0);
				LOGGER.info("initialized id allocator, current generation is "
						+ this.currentGeneration);
			} finally {
				if (transaction != null) {
					transaction.abort();
				}
			}
		}

		public ObjectId allocate() {
			Long newIdentity = this.currentIdentity.incrementAndGet();
			ObjectId newId = new ObjectId(domain, this.currentGeneration,
					newIdentity);
			LOGGER.debug("SUCCEEDED: " + newId + ": allocated");
			return newId;
		}

		public void free(ObjectId id) {
			// Since we are not going to reuse id, silently do nothing.
			LOGGER.debug("SUCCEEDED: " + id + ": freed");
		}
	}

	ObjectId allocate() throws IllegalStateException {
		this.checkRunning();
		return this.idAllocator.allocate();
	}

	void free(ObjectId id) throws IllegalStateException {
		this.checkRunning();
		this.idAllocator.free(id);
	}

	private class StatisSummer extends Thread {
		private final Logger LOGGER = Logger.getLogger(StatisSummer.class);

		public static final int SLEEP_MS = 300000; // 5 min
		public static final int MAX_RUNNING_SUMMER = 5;

		private SummerThread[] runningSummer = new SummerThread[MAX_RUNNING_SUMMER];

		public void run() {
			while (!shouldStop.get()) {
				for (int i = 0; i < MAX_RUNNING_SUMMER; i++) {
					if (this.runningSummer[i] != null
							&& !this.runningSummer[i].isRunning()) {
						this.runningSummer[i] = null;
					}
				}

				for (int i = 0; i < MAX_RUNNING_SUMMER; i++) {
					if (this.runningSummer[i] == null) {
						this.runningSummer[i] = new SummerThread();
						this.runningSummer[i].start();
						this.runningSummer[i].setName("summer thread " + i);
						LOGGER.info("started a new summer thread: "
								+ this.runningSummer[i].getName());
						break;
					}
				}

				try {
					sleep(SLEEP_MS);
				} catch (InterruptedException e) {
					// Ignore
				}
			}
		}

		public void close() {
			LOGGER.info("closing usage summer thread");

			this.interrupt();

			final int MAX_RETRIES = 10;
			int retries = 0;

			while (retries != MAX_RETRIES) {
				try {
					this.join();
					break;
				} catch (InterruptedException e) {
					retries++;
				}
			}

			if (retries == MAX_RETRIES) {
				LOGGER.warn("usage summer thread failed to join main thread");
			}

			for (SummerThread summer : this.runningSummer) {
				if (summer == null) {
					continue;
				}
				retries = 0;
				while (retries != MAX_RETRIES) {
					try {
						summer.join();
						break;
					} catch (InterruptedException e) {
						retries++;
					}
				}

				if (retries == MAX_RETRIES) {
					LOGGER.warn("one summer thread failed to join main thread");
				}
			}

			LOGGER.info("closed usage summer thread");
		}

		private class SummerThread extends Thread {
			private final Logger LOGGER = Logger.getLogger(SummerThread.class);

			private AtomicBoolean isRunning = new AtomicBoolean(true);

			private Statistic sumUpDirectory(ObjectId directoryId)
					throws DatabaseException, IOException {
				boolean ret = false;
				Database treeDb = databaseManager.getDatabase(TREE_DB_NAME);
				Database metaDb = databaseManager.getDatabase(META_DB_NAME);
				Database statisDb = databaseManager.getDatabase(STATIS_DB_NAME);
				Cursor cursor = null;

				try {
					ObjectId directoryHandleId = new ObjectId();
					ret = DatabaseUtils.get(treeDb, null, new CompoundKey(
							directoryId, FileObject.DIRECTORY_KEY_SUFFIX),
							directoryHandleId);
					if (!ret) {
						return null;
					}

					cursor = databaseManager.openCursor(treeDb, null);

					ret = DatabaseUtils.getSearchKey(treeDb, cursor,
							new CompoundKey(directoryHandleId,
									FileObject.HANDLE_KEY_SUFFIX), null);
					if (!ret) {
						return null;
					}

					ObjectId objectId = new ObjectId();
					CompoundKey entryKey = new CompoundKey(new ObjectId(), null);
					Meta meta = new Meta();
					long aggregateSize = 0L;
					long numberOfFiles = 0L;
					long numberOfDirectories = 0L;
					Statistic subDirectoryUsage = null;
					Statistic statis = new Statistic();
					statis.setStartTime(new Date().getTime());

					while (!shouldStop.get()) {
						ret = DatabaseUtils.getNextMatch(treeDb, cursor,
								directoryHandleId, entryKey, objectId);
						if (!ret) {
							break;
						}

						ret = DatabaseUtils.get(metaDb, null, objectId, meta);
						if (!ret) {
							continue;
						}

						if (meta.isDirectory()) {
							subDirectoryUsage = sumUpDirectory(objectId);
							if (subDirectoryUsage == null) {
								continue;
							}

							numberOfDirectories++;
							aggregateSize += subDirectoryUsage
									.getAggregateSize();
							numberOfFiles += subDirectoryUsage
									.getNumberOfFiles();
							numberOfDirectories += subDirectoryUsage
									.getNumberOfDirectories();
						} else {
							numberOfFiles++;
							long size = meta.getSize();
							if (size > 1024L * 1024L * 1024L * 100L) {
								LOGGER.info(String
										.format("ALERT!! Size %d, objectId %s, path %s",
												size, objectId.toString(),
												meta.getName()));
							} else {
								aggregateSize += size;
							}
						}
					}
					cursor.close();
					cursor = null;

					statis.setEndTime(new Date().getTime());
					statis.setAggregateSize(aggregateSize);
					statis.setNumberOfFiles(numberOfFiles);
					statis.setNumberOfDirectories(numberOfDirectories);

					DatabaseUtils.put(statisDb, null, directoryId, statis);
					return statis;
				} finally {
					if (cursor != null) {
						cursor.close();
					}
				}
			}

			public void run() {
				try {
					sumUpDirectory(FS_ROOT_ID);
					this.isRunning.set(false);
					LOGGER.info(this.getName() + " finished");
				} catch (DatabaseException e) {
					LOGGER.error("database exception", e);
				} catch (IOException e) {
					LOGGER.error("io exception", e);
				} finally {
					this.isRunning.set(false);
				}
			}

			public boolean isRunning() {
				return this.isRunning.get();
			}
		}
	}

	private class GarbageCollector extends Thread {
		private final Logger LOGGER = Logger.getLogger(GarbageCollector.class);

		private static final int SLEEP_MS = 300000; // 5 min

		private LinkedBlockingQueue<ObjectId> garbageQueue = new LinkedBlockingQueue<ObjectId>();
		private Stack<ObjectId> processingStack = new Stack<ObjectId>();

		public void run() {
			boolean ret = false;
			Database metaDb = databaseManager.getDatabase(META_DB_NAME);
			Database extMetaDb = databaseManager.getDatabase(EXT_META_DB_NAME);

			try {
				while (!shouldStop.get()) {
					if (this.garbageQueue.isEmpty()) {
						try {
							sleep(SLEEP_MS);
						} catch (InterruptedException e) {
							// Ignore
						}
						continue;
					}

					this.processingStack.push(this.garbageQueue.poll());
					while (!this.processingStack.isEmpty()) {
						ObjectId objectId = this.processingStack.pop();

						// Delete extended meta-data, never mind if this fails
						// since extended meta-data may not exists for this
						// file.
						DatabaseUtils.delete(extMetaDb, null, objectId);

						// Get and delete meta-data.
						Meta meta = new Meta();
						ret = DatabaseUtils.getAndDelete(metaDb, null,
								objectId, meta);
						if (!ret) {
							LOGGER.warn("some garbages are not successfully collected");
							continue;
						}

						if (meta.isDirectory()) {
							this.recycleDirectory(objectId);
							LOGGER.debug("directory: " + objectId + " recycled");
						} else {
							this.recycleFile(objectId);
							LOGGER.debug("file: " + objectId + " recycled");
						}
					}
				}
			} catch (DatabaseException e) {
				LOGGER.error("database exception", e);
				return;
			} catch (IOException e) {
				LOGGER.error("io exception", e);
			}
		}

		public void close() {
			LOGGER.info("closing garbage collector thread");

			this.interrupt();

			final int MAX_RETRIES = 10;
			int retries = 0;

			while (retries != MAX_RETRIES) {
				try {
					this.join();
					break;
				} catch (InterruptedException e) {
					retries++;
				}
			}

			if (retries == MAX_RETRIES) {
				LOGGER.warn("garbage collector thread failed to join main thread");
			}

			if (!this.garbageQueue.isEmpty() || !this.processingStack.isEmpty()) {
				LOGGER.warn("some garbages are not successfully collected");
			}

			LOGGER.info("closed garbage collector thread");
		}

		public void addGarbage(ObjectId id) {
			this.garbageQueue.add(id);
			LOGGER.debug("SUCCEEDED: " + id + " added as garbage");
		}

		public void addGarbages(Collection<ObjectId> ids) {
			this.garbageQueue.addAll(ids);
			LOGGER.debug("SUCCEEDED: " + ids + " added as garbages");
		}

		private void recycleFile(ObjectId fileId) throws DatabaseException,
				IOException {
			boolean ret = false;
			Database distributionDb = databaseManager
					.getDatabase(DISTRIBUTION_DB_NAME);

			// Delete file distribution.
			ret = DatabaseUtils.delete(distributionDb, null, fileId);
			if (!ret) {
				LOGGER.warn("some garbages are not successfully collected");
			}
		}

		private void recycleDirectory(ObjectId dirId) throws DatabaseException,
				IOException {
			boolean ret = false;
			Database treeDb = databaseManager.getDatabase(TREE_DB_NAME);
			Transaction transaction = null;
			Cursor cursor = null;

			// Delete directory id.
			ObjectId dirHandleId = new ObjectId();
			ret = DatabaseUtils.getAndDelete(treeDb, null, new CompoundKey(
					dirId, FileObject.DIRECTORY_KEY_SUFFIX), dirHandleId);
			if (!ret) {
				LOGGER.warn("some garbages are not successfully collected");
				return;
			}

			try {
				transaction = databaseManager.beginTransaction();
				cursor = databaseManager.openCursor(treeDb, transaction);

				// Delete directory handle id.
				ret = DatabaseUtils.getSearchKeyAndDelete(treeDb, cursor,
						new CompoundKey(dirHandleId,
								FileObject.HANDLE_KEY_SUFFIX), null);
				if (!ret) {
					LOGGER.warn("some garbages are not successfully collected");
					return;
				}

				// Delete objects under directory.
				CompoundKey entryKey = new CompoundKey(new ObjectId(), null);
				ObjectId objectId = new ObjectId();
				while (true) {
					ret = DatabaseUtils.getNextMatchAndDelete(treeDb, cursor,
							dirHandleId, entryKey, objectId);
					if (!ret) {
						break;
					}

					this.processingStack.push(objectId.clone());
				}

				cursor.close();
				cursor = null;
				transaction.commit();
				transaction = null;
			} finally {
				if (cursor != null) {
					cursor.close();
				}
				if (transaction != null) {
					transaction.abort();
				}
			}
		}
	}

	void addGarbage(ObjectId id) throws IllegalStateException {
		this.checkRunning();
		this.garbageCollector.addGarbage(id);
	}

	void addGarbages(Collection<ObjectId> ids) throws IllegalStateException {
		this.checkRunning();
		this.garbageCollector.addGarbages(ids);
	}
}
