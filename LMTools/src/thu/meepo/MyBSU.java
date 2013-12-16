package thu.meepo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.meepo.hyla.FileObject;
import org.meepo.hyla.FileSystem;
import org.meepo.hyla.Meta;
import org.meepo.hyla.dist.DataSegment;
import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.io.CompoundKey;
import org.meepo.hyla.io.ObjectId;
import org.meepo.hyla.util.DatabaseUtils;
import org.meepo.hyla.util.IOUtils;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class MyBSU {

	protected static final String HEADER_DB_NAME = "HeaderDb";
	protected static final String TREE_DB_NAME = "TreeDb";
	protected static final String META_DB_NAME = "MetaDb";
	protected static final String EXT_META_DB_NAME = "ExtMetaDb";
	protected static final String DISTRIBUTION_DB_NAME = "DistributionDb";
	protected static final String STORAGE_DB_NAME = "StorageDb";
	protected static final String STATIS_DB_NAME = "StatisDb";

	private String homeDir = "";
	private String hylaName = "hyla-db";
	private String outName = "out";
	private String loggerName = "logger";

	private PrintWriter pw;
	private PrintWriter logger;
	private TransactionConfig transactionConfig;
	private CursorConfig cursorConfig;

	private Environment dbEnv = null;
	private Database treeDb = null;
	private Database metaDb = null;
	private Database distDb = null;
	// private Database extMetaDb = null;

	Transaction transaction = null;

	public MyBSU(String hdir) {
		this.homeDir = hdir;
	}

	private void initialize() {
		this.transactionConfig = new TransactionConfig();
		this.transactionConfig.setReadCommitted(true);
		this.cursorConfig = new CursorConfig();
		this.cursorConfig.setReadCommitted(true);

		try {
			EnvironmentConfig dbEnvConfig = null;
			dbEnvConfig = new EnvironmentConfig();
			dbEnvConfig.setAllowCreate(false);
			dbEnvConfig.setTransactional(true);

			File home = new File(homeDir + File.separator + hylaName);

			this.dbEnv = new Environment(home, dbEnvConfig);

			DatabaseConfig dbConfig = null;
			dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(false);
			dbConfig.setTransactional(true);
			dbConfig.setSortedDuplicates(false);
			dbConfig.setCacheMode(CacheMode.KEEP_HOT);

			this.treeDb = this.dbEnv.openDatabase(null, TREE_DB_NAME, dbConfig);
			this.metaDb = this.dbEnv.openDatabase(null, META_DB_NAME, dbConfig);
			this.distDb = this.dbEnv.openDatabase(null, DISTRIBUTION_DB_NAME,
					dbConfig);
			// this.extMetaDb = this.dbEnv.openDatabase(null, EXT_META_DB_NAME,
			// dbConfig);

			cursorConfig = new CursorConfig();
			cursorConfig.setReadCommitted(true);

			pw = new PrintWriter(new FileWriter(homeDir + File.separator
					+ outName));
			logger = new PrintWriter(new FileWriter(homeDir + File.separator
					+ loggerName));

		} catch (DatabaseException e) {
			this.close();
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void close() {
		if (this.treeDb != null) {
			this.treeDb.close();
			this.treeDb = null;
		}

		if (this.metaDb != null) {
			this.metaDb.close();
			this.metaDb = null;
		}

		if (this.distDb != null) {
			this.distDb.close();
			this.distDb = null;
		}

		if (this.pw != null) {
			pw.flush();
			pw.close();
		}
	}

	public Distribution getFileDistribution(ObjectId objId)
			throws IllegalStateException, IOException {

		if (objId == null) {
			this.logger.write("FAILED id not found \n");
			return null;
		}

		boolean ret = false;

		Distribution dist = new Distribution();
		try {
			ret = DatabaseUtils.get(this.distDb, null, objId, dist);
			if (!ret) {
				this.logger.write("FAILED  object not exists\n");
				return null;
			}
		} catch (DatabaseException e) {
			this.logger.write("database exception\n");
			throw new IOException(e);
		}
		return dist;
	}

	public void writeln(String str) {
		this.pw.write(str + "\n");
	}

	public void logln(String str) {
		this.logger.write(str + "\n");
		this.logger.flush();
	}

	@SuppressWarnings("unused")
	private void iterateSingleDb(Database db) {
		if (db == null) {
			return;
		}

		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbValue = new DatabaseEntry();

		ObjectId keyEntity = new ObjectId();
		Distribution valueEntity = new Distribution();

		Cursor cursor = db.openCursor(null, this.cursorConfig);

		try {
			while (cursor.getNext(dbKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				// System.out.println(dbKey.toString());
				IOUtils.deserialize(dbKey.getData(), keyEntity);
				IOUtils.deserialize(dbValue.getData(), valueEntity);
				DataSegment dsg[] = valueEntity.getDataSegments();
				String ds_path = "./" + dsg[0].getPathOnStorage();
				this.writeln(keyEntity.toString() + "\t" + ds_path);
			}
			cursor.close();
			cursor = null;
		} catch (Exception e) {
			e.printStackTrace();
			this.logln(e.getMessage());
		} finally {
			if (cursor != null) {
				cursor.close();
				cursor = null;
			}
			this.pw.flush();
			this.logger.flush();
		}

	}

	private void visitOne(ObjectId objectId, String pname, int depth) {
		Cursor cursor = null;
		boolean ret;
		// boolean crack = false;

		if (depth >= 3) {
			return;
		}

		try {
			cursor = treeDb.openCursor(null, this.cursorConfig);
			String objName = "";
			// long objSize = 0;
			// long objCreateTime = 0;
			Meta objMeta = new Meta();
			boolean isDir = false;
			ret = DatabaseUtils.get(metaDb, null, objectId, objMeta);
			// if (!ret) {
			// crack = true;
			// }
			objName = objMeta.getName();
			// objSize = objMeta.getSize();
			// objCreateTime = objMeta.getCreateTime();
			pname = pname + "/" + objName;
			String objNewName = objName;

			if (pname.startsWith("//.Trash/Groups/")
					|| pname.startsWith("//.Trash/Public/")
					|| pname.startsWith("//.tmp/")
					|| pname.startsWith("//Groups/")
					|| pname.startsWith("//MySpace/")) {
				return; // XXX 通过与depth类似方法设置目录名排除
			} else if (pname.startsWith("")) {

			}

			// Change name
			if (objNewName.contains("@")) {
				String[] parts = objNewName.split("@");
				objNewName = "";
				for (int i = 0; i < parts.length - 1; i++) {
					objNewName = objNewName + parts[i];
				}
			}

			// System.out.println(objNewName);
			this.writeln(pname + "\t" + objName + "\t" + objNewName);
			// Update to meta
			ObjectId objHandleId = new ObjectId();
			ret = DatabaseUtils.get(treeDb, transaction, new CompoundKey(
					objectId, "*"), objHandleId);
			isDir = ret;
			if (!isDir) {
				String pw_name = pname.substring(1);
				// get file create time and file size and file abs name
				pw.write(pw_name + "\n");
			}

			if (isDir) {
				CompoundKey entryKey = new CompoundKey(new ObjectId(), null);
				ObjectId sonObjectId = new ObjectId();

				ret = DatabaseUtils.getSearchKey(treeDb, cursor,
						new CompoundKey(objHandleId,
								FileObject.HANDLE_KEY_SUFFIX), null);

				while (true) {
					ret = DatabaseUtils.getNextMatch(treeDb, cursor,
							objHandleId, entryKey, sonObjectId);
					if (!ret) {
						break;
					}

					this.visitOne(sonObjectId, pname, depth + 1);

				}
			}

			cursor.close();
			cursor = null;
			return;

		} catch (DatabaseException e) {
			System.out.println("DBexception ");
			e.printStackTrace();
			this.logln(e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException");
			e.printStackTrace();
			this.logln(e.getMessage());
			// } catch (NoSuchAlgorithmException e) {
			// System.out.println("NOSuch ALgorithm");
			// e.printStackTrace();
		} finally {
			if (cursor != null) {
				cursor.close();
			}
			// pw.flush();
			// System.out.println("visit\t" + pname );
		}

	}

	private void VisitAllNames() {
		visitOne(FileSystem.FS_ROOT_ID, "", 0);
		pw.flush();
		pw.close();
	}

	public static void main(String args[]) {
		// String hdir = args[0];
		String hdir = "..";
		MyBSU go = new MyBSU(hdir);
		go.initialize();

		// go.iterateSingleDb(go.distDb);
		go.VisitAllNames();

		go.close();
	}
}
