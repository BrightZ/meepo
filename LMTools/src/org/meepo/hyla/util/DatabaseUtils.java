package org.meepo.hyla.util;

import java.io.IOException;

import org.meepo.hyla.io.CompoundKey;
import org.meepo.hyla.io.Writable;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DatabaseUtils {
	public static boolean put(Database database, Transaction transaction,
			Writable keyObj, Writable dataObj) throws DatabaseException,
			IOException {
		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbData = new DatabaseEntry();

		dbKey.setData(IOUtils.serialize(keyObj));
		dbData.setData(IOUtils.serialize(dataObj));

		database.put(transaction, dbKey, dbData);
		return true;

	}

	public static boolean putNoOverwrite(Database database,
			Transaction transaction, Writable keyObj, Writable dataObj)
			throws DatabaseException, IOException {
		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbData = new DatabaseEntry();

		dbKey.setData(IOUtils.serialize(keyObj));
		dbData.setData(IOUtils.serialize(dataObj));

		OperationStatus opStat = database.putNoOverwrite(transaction, dbKey,
				dbData);
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}
		return true;
	}

	public static boolean get(Database database, Transaction transaction,
			Writable keyObj, Writable dataObj) throws DatabaseException,
			IOException {
		return get(database, transaction, keyObj, dataObj,
				LockMode.READ_UNCOMMITTED);
	}

	public static boolean lock(Database database, Transaction transaction,
			Writable keyObj) throws DatabaseException, IOException {
		return get(database, transaction, keyObj, null, LockMode.RMW);
	}

	public static boolean getAndLock(Database database,
			Transaction transaction, Writable keyObj, Writable dataObj)
			throws DatabaseException, IOException {
		return get(database, transaction, keyObj, dataObj, LockMode.RMW);
	}

	public static boolean delete(Database database, Transaction transaction,
			Writable keyObj) throws DatabaseException, IOException {
		DatabaseEntry dbKey = new DatabaseEntry();

		dbKey.setData(IOUtils.serialize(keyObj));

		OperationStatus opStat = database.delete(transaction, dbKey);
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}
		return true;
	}

	public static boolean getAndDelete(Database database,
			Transaction transaction, Writable keyObj, Writable dataObj)
			throws DatabaseException, IOException {
		boolean ret;

		ret = get(database, transaction, keyObj, dataObj);
		if (!ret) {
			return ret;
		}
		return delete(database, transaction, keyObj);
	}

	private static boolean get(Database database, Transaction transaction,
			Writable keyObj, Writable dataObj, LockMode lockMode)
			throws DatabaseException, IOException {
		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbData = new DatabaseEntry();

		dbKey.setData(IOUtils.serialize(keyObj));

		OperationStatus opStat = database.get(transaction, dbKey, dbData,
				lockMode);
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}

		if (dataObj != null) {
			IOUtils.deserialize(dbData.getData(), dataObj);
		}
		return true;
	}

	/*---------------------------------------------------------------------------*/

	public static boolean getSearchKey(Database database, Cursor cursor,
			Writable keyObj, Writable dataObj) throws DatabaseException,
			IOException {
		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbData = new DatabaseEntry();

		dbKey.setData(IOUtils.serialize(keyObj));

		OperationStatus opStat = cursor.getSearchKey(dbKey, dbData,
				LockMode.READ_UNCOMMITTED);
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}

		if (dataObj != null) {
			IOUtils.deserialize(dbData.getData(), dataObj);
		}
		return true;
	}

	public static boolean delete(Database database, Cursor cursor)
			throws DatabaseException {
		OperationStatus opStat = cursor.delete();
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}
		return true;
	}

	public static boolean getSearchKeyAndDelete(Database database,
			Cursor cursor, Writable keyObj, Writable dataObj)
			throws DatabaseException, IOException {
		if (!getSearchKey(database, cursor, keyObj, dataObj)) {
			return false;
		}
		return delete(database, cursor);
	}

	public static boolean getNext(Database database, Cursor cursor,
			Writable keyObj, Writable dataObj) throws DatabaseException,
			IOException {
		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbData = new DatabaseEntry();

		OperationStatus opStat = cursor.getNext(dbKey, dbData,
				LockMode.READ_UNCOMMITTED);
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}

		if (keyObj != null) {
			IOUtils.deserialize(dbKey.getData(), keyObj);
		}
		if (dataObj != null) {
			IOUtils.deserialize(dbData.getData(), dataObj);
		}
		return true;
	}

	public static boolean getNextAndDelete(Database database, Cursor cursor,
			Writable keyObj, Writable dataObj) throws DatabaseException,
			IOException {
		if (!getNext(database, cursor, keyObj, dataObj)) {
			return false;
		}
		return delete(database, cursor);
	}

	public static boolean getNextMatch(Database database, Cursor cursor,
			Writable keyPrefixCond, CompoundKey keyObj, Writable dataObj)
			throws DatabaseException, IOException {
		DatabaseEntry dbKey = new DatabaseEntry();
		DatabaseEntry dbData = new DatabaseEntry();

		OperationStatus opStat = cursor.getNext(dbKey, dbData,
				LockMode.READ_UNCOMMITTED);
		if (opStat != OperationStatus.SUCCESS) {
			return false;
		}

		if (keyObj != null) {
			IOUtils.deserialize(dbKey.getData(), keyObj);
		}
		if (dataObj != null) {
			IOUtils.deserialize(dbData.getData(), dataObj);
		}

		return keyObj.getPrefix().equals(keyPrefixCond);
	}

	public static boolean getNextMatchAndDelete(Database database,
			Cursor cursor, Writable keyPrefixCond, CompoundKey keyObj,
			Writable dataObj) throws DatabaseException, IOException {
		if (!getNextMatch(database, cursor, keyPrefixCond, keyObj, dataObj)) {
			return false;
		}
		return delete(database, cursor);
	}
}
