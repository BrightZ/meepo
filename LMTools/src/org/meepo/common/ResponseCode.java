package org.meepo.common;

/**
 * This defines the error codes.
 * 
 * @author MS
 * 
 */
public final class ResponseCode {
	/**
	 * Succeeded.
	 * */
	public static final int SUCCESS = 1000;

	/**
	 * The given user does not exist in database.
	 * */
	public static final int USER_NOT_EXISTS = 1001;

	/**
	 * The given user exists but has not been activated yet.
	 * */
	public static final int USER_NOT_ACTIVATED = 1002;

	/**
	 * The given user exists but has been banned.
	 * */
	public static final int USER_BANNED = 1003;

	/**
	 * The given user exists but the given password is incorrect.
	 * */
	public static final int PASSWD_INCORRECT = 1004;

	/**
	 * The given token does not exist.
	 * */
	public static final int TOKEN_INCORRECT = 1005;

	/**
	 * The given token has been timed out.
	 * */
	public static final int TOKEN_TIMEOUT = 1006;

	/**
	 * The given path is incorrect.
	 * */
	public static final int PATH_INCORRECT = 1007;

	/**
	 * The given user does not have right permission to finish the operation.
	 * */
	public static final int PERMISSION_DENIED = 1008;

	/**
	 * The given path already exists when trying to create it.
	 * */
	public static final int FILE_DIR_ALREADY_EXISTS = 1009;

	/**
	 * Parent of the given path does not exist.
	 */
	public static final int PARENT_DIR_NOT_EXISTS = 1010;

	/**
	 * Operation invalid, usually parameters not correct.
	 */
	public static final int INVALID_OPERATION = 1011;

	/**
	 * You are not allowed to make changes to this path.
	 */
	public static final int PATH_ACCESS_DENIED = 1012;

	/**
	 * This path is read-only to non-administrators.
	 */
	public static final int ACCESS_READ_ONLY = 1013;

	/**
	 * This path is writable to every group members
	 */
	public static final int ACCESS_WRITE_ALLOW = 1014;

	/**
	 * You are not administrator and cannot modify access rights
	 */
	public static final int ACCESS_NOT_ADMIN = 1015;

	/**
	 * For now you can only set access rules to directories, not files.
	 */
	public static final int ACCESS_NOT_DIRECTORY = 1016;

	/**
	 * The given path does not exist when trying to remove it.
	 * */
	public static final int FILE_DIR_NOT_EXISTS = 1017;

	/**
	 * 
	 */
	public static final int DIR_NOT_EMPTY = 1018;

	/**
	 * 
	 */
	public static final int MOVE_NOT_SAME_PARTITION = 1019;

	/**
	 * 
	 */
	public static final int LOCK_CONFLICT = 1020;

	/**
	 * 
	 */
	public static final int USER_GROUP_NONE = 1021;

	/**
	 * 
	 */
	public static final int USER_GROUP_ADMIN = 1022;

	/**
	 * 
	 */
	public static final int USER_GROUP_MEMEBER = 1023;

	/**
	 * 
	 */
	public static final int SPACE_FULL = 1024;

	/**
	 * 
	 */
	public static final int ACCESS_UPLOAD = 1025;

	/**
	 * The version you are using is too old that it's no longer supported.
	 */
	public static final int VERSION_NOT_SUPPORTED = 1026;

	/**
	 * The cipher of the corresponding id does not exit.
	 */
	public static final int CIPHER_NOT_EXIST = 1027;

	/**
	 * System is stopped due to a manually shutdown, please pray it will start
	 * soon.
	 * */
	public static final int SYSTEM_STOP = 0xfffd;

	/**
	 * Serious system error, may God help us.
	 * */
	public static final int SYSTEM_ERROR = 0xfffe; // Meepo system error

	/**
	 * Unknown error, this system is gonna crash into pieces and never come back
	 * again.
	 * */
	public static final int UNKNOWN_ERROR = 0xffff; // Unknown error
}
