package org.meepo.hyla.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;

import org.meepo.hyla.FileObject;
import org.meepo.hyla.FileSystem;
import org.meepo.hyla.Meta;
import org.meepo.hyla.OperationResponse;
import org.meepo.hyla.dist.DefaultDistributionPolicy;
import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.storage.FTPStorage;

public class SimpleFSShell {
	public static void main(String[] args) {
		if (args.length == 0) {
			// new SimpleFSShell().run();
			new SimpleFSShell("/home/zork/sandbox/bsu/hyla-db").run();
		} else {
			new SimpleFSShell(args[0]).run();
		}
	}

	protected static String[] parseCommand(String cmd) {
		ArrayList<String> segs = new ArrayList<String>();
		StringBuilder segBuilder = new StringBuilder();

		char currentChar;
		for (int i = 0; i < cmd.length(); i++) {
			currentChar = cmd.charAt(i);
			if (currentChar == '\\') {
				if (i == cmd.length() - 1) {
					return null;
				}
				if (cmd.charAt(i + 1) != ' ') {
					return null;
				}

				segBuilder.append(" ");
				i++;
				continue;
			}

			if (currentChar == ' ') {
				if (segBuilder.length() != 0) {
					segs.add(segBuilder.toString());
				}
				segBuilder = new StringBuilder();
				continue;
			}

			segBuilder.append(currentChar);
		}

		if (segBuilder.length() != 0) {
			segs.add(segBuilder.toString());
		}
		return segs.toArray(new String[0]);
	}

	public static final long KILO_BYTE = 1024L;
	public static final long MEGA_BYTE = 1024L * KILO_BYTE;
	public static final long GIGA_BYTE = 1024L * MEGA_BYTE;
	public static final long TERA_BYTE = 1024L * GIGA_BYTE;

	protected static String getSizeString(long size) {
		long number;
		String unit;
		if (size < KILO_BYTE) {
			number = size;
			unit = " B ";
		} else if (size < MEGA_BYTE) {
			number = size / KILO_BYTE;
			unit = " KB";
		} else if (size < GIGA_BYTE) {
			number = size / MEGA_BYTE;
			unit = " MB";
		} else if (size < TERA_BYTE) {
			number = size / GIGA_BYTE;
			unit = " TB";
		} else {
			number = size / TERA_BYTE;
			unit = " PB";
		}

		if (number < 10) {
			return "   " + number + unit;
		} else if (number < 100) {
			return "  " + number + unit;
		} else if (number < 1000) {
			return " " + number + unit;
		} else {
			return number + unit;
		}
	}

	protected static String getTimeString(long time) {
		Calendar date = Calendar.getInstance();
		date.setTimeInMillis(time);
		StringBuilder timeStringBuilder = new StringBuilder();

		timeStringBuilder.append(date.get(Calendar.YEAR) + " ");

		String[] month = new String[] { "Jan", "Feb", "Mar", "Apr", "May",
				"Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
		timeStringBuilder.append(month[date.get(Calendar.MONTH)] + " ");

		int day = date.get(Calendar.DAY_OF_MONTH);
		if (day < 10) {
			timeStringBuilder.append(" " + day + " ");
		} else {
			timeStringBuilder.append(day + " ");
		}

		int hour = date.get(Calendar.HOUR_OF_DAY);
		if (hour < 10) {
			timeStringBuilder.append("0" + hour + ":");
		} else {
			timeStringBuilder.append(hour + ":");
		}

		int minute = date.get(Calendar.MINUTE);
		if (minute < 10) {
			timeStringBuilder.append("0" + minute);
		} else {
			timeStringBuilder.append(minute);
		}

		return timeStringBuilder.toString();
	}

	private FileSystem fileSystem;
	private String workingDirectory = FileSystem.SEPARATOR;

	public SimpleFSShell() {
		this("./");
	}

	public SimpleFSShell(String fsLocation) {
		try {
			this.fileSystem = new FileSystem(new File(fsLocation), true);
			this.fileSystem.addStorage(new FTPStorage("", 0, "", ""));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		BufferedReader stdin = new BufferedReader(new InputStreamReader(
				System.in));

		while (true) {
			try {
				System.out.print("[" + this.workingDirectory + "]$ ");
				String cmd = stdin.readLine();
				if (cmd.trim().length() == 0) {
					continue;
				}

				if (cmd.equals("exit") || cmd.equals("quit")) {
					this.fileSystem.close();
					System.exit(0);
				}

				this.execute(cmd);
			} catch (IllegalStateException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		}
	}

	private void execute(String cmd) throws IllegalStateException, IOException {
		String[] segs = parseCommand(cmd);
		if (segs == null) {
			System.out.println("illegal command format");
			return;
		}
		if (segs.length == 0) {
			System.out.println("illegal command format");
			return;
		}
		String operation = segs[0];

		String absPath = null;
		if (segs.length > 1) {
			absPath = this.covertToAbsPath(segs[1]);
		}

		if (operation.equals("ls") || operation.equals("dir")) {
			if (absPath == null) {
				this.list(this.workingDirectory);
			} else {
				this.list(absPath);
			}
		} else if (operation.equals("cd")) {
			if (absPath == null) {
				this.changeDirectory(FileSystem.SEPARATOR);
			} else {
				this.changeDirectory(absPath);
			}
		} else if (operation.equals("touch")) {
			if (absPath == null) {
				System.out.println("touch: missing file operand");
			} else {
				this.createFile(absPath);
			}
		} else if (operation.equals("mkdir")) {
			if (absPath == null) {
				System.out.println("mkdir: missing file operand");
			} else {
				this.makeDirectory(absPath);
			}
		} else if (operation.equals("rm")) {
			if (absPath == null) {
				System.out.println("rm: missing file operand");
			} else {
				this.delete(absPath);
			}
		} else if (operation.equals("mv")) {
			if (absPath == null) {
				System.out.println("mv: missing file operand");
			} else if (segs.length < 3) {
				System.out.println("mv: insufficient file operand");
			} else {
				this.move(absPath, this.covertToAbsPath(segs[2]));
			}
		} else if (operation.equals("get")) {
			if (absPath == null) {
				System.out.println("get: missing file operand");
			} else {
				this.get(absPath);
			}
		} else if (operation.equals("put")) {
			if (absPath == null) {
				System.out.println("put: missing file operand");
			} else {
				this.put(absPath);
			}
		} else {
			System.out.println("unknown command");
		}
	}

	private String covertToAbsPath(String path) throws IOException {
		if (!path.startsWith(FileSystem.SEPARATOR)) {
			path = this.workingDirectory + FileSystem.SEPARATOR + path;
		}
		return this.fileSystem.openObject(path).getRealPath();
	}

	private void list(String absPath) throws IllegalStateException, IOException {
		FileObject object = this.fileSystem.openObject(absPath);
		if (!object.isDirectory()) {
			System.out.println(absPath + ": no such directory");
			return;
		}

		System.out
				.println("Type  Size       Create Time          Modify Time          Name");
		FileObject[] objects = object.list();
		for (int i = 0; i < objects.length; i++) {
			Meta meta = objects[i].getAttributes();
			if (meta == null) {
				continue;
			}

			if (meta.isDirectory()) {
				System.out.print("d     ");
			} else {
				System.out.print("f     ");
			}

			System.out.println(getSizeString(meta.getSize()) + "    "
					+ getTimeString(meta.getCreateTime()) + "    "
					+ getTimeString(meta.getModifyTime()) + "    "
					+ meta.getName());
		}
	}

	private void changeDirectory(String absPath) throws IllegalStateException,
			IOException {
		FileObject object = this.fileSystem.openObject(absPath);
		if (!object.isDirectory()) {
			System.out.println(absPath + ": no such directory");
			return;
		}
		this.workingDirectory = absPath;
	}

	private void createFile(String absPath) throws IllegalStateException,
			IOException {
		FileObject file = this.fileSystem.openObject(absPath);
		OperationResponse opResp = file
				.createFile(new DefaultDistributionPolicy());

		switch (opResp) {
		case SUCCESS:
			System.out.println(absPath + ": created");
			break;
		case OBJECT_ALREADY_EXISTS:
			System.out.println(absPath + ": already exists");
			break;
		default:
			System.out.println("unexpected error");
			break;
		}
	}

	private void makeDirectory(String absPath) throws IllegalStateException,
			IOException {
		FileObject directory = this.fileSystem.openObject(absPath);
		OperationResponse opResp = directory.makeDirectory();

		switch (opResp) {
		case SUCCESS:
			System.out.println(absPath + ": created");
			break;
		case OBJECT_ALREADY_EXISTS:
			System.out.println(absPath + ": already exists");
			break;
		default:
			System.out.println("unexpected error");
			break;
		}
	}

	private void delete(String absPath) throws IllegalStateException,
			IOException {
		FileObject object = this.fileSystem.openObject(absPath);
		OperationResponse opResp = object.delete();

		switch (opResp) {
		case SUCCESS:
			System.out.println(absPath + ": deleted");
			break;
		case OBJECT_NOT_EXISTS:
			System.out.println(absPath + ": no such file or directory");
			break;
		default:
			System.out.println("unexpected error");
			break;
		}
	}

	private void move(String absSrcPath, String absDstPath)
			throws IllegalStateException, IOException {
		FileObject srcObj = this.fileSystem.openObject(absSrcPath);
		FileObject dstObj = this.fileSystem.openObject(absDstPath);
		OperationResponse opResp = srcObj.moveTo(dstObj);

		switch (opResp) {
		case SUCCESS:
			System.out.println(absSrcPath + ": moved to " + absDstPath);
			break;
		case OBJECT_NOT_EXISTS:
			System.out.println(absSrcPath + ": no such file or directory");
			break;
		case OBJECT_ALREADY_EXISTS:
			System.out.println(absDstPath + ": already exists");
			break;
		case PARENT_NOT_EXISTS:
			System.out.println(absDstPath + ": parent folder not exists");
			break;
		default:
			System.out.println("unexpected error");
			break;
		}
	}

	private void get(String absPath) throws IllegalStateException, IOException {
		FileObject object = this.fileSystem.openObject(absPath);
		Meta meta = object.getAttributes();
		if (meta == null || meta.isDirectory()) {
			System.out.println(absPath + ": no such file");
			return;
		}

		Distribution dist = object.getFileDistribution();
		if (dist == null) {
			System.out.println(absPath + ": no such file");
			return;
		}
		System.out.println(dist);
		System.out.println(this.fileSystem.getStorage(dist.getDataSegments()[0]
				.getStorageId()));
	}

	private void put(String absPath) throws IllegalStateException, IOException {
		FileObject object = this.fileSystem.openObject(absPath);
		Meta meta = object.getAttributes();
		if (meta == null || meta.isDirectory()) {
			System.out.println(absPath + ": no such file");
			return;
		}

		Distribution dist = object.getFileDistribution();
		if (dist == null) {
			System.out.println(absPath + ": no such file");
			return;
		}
		System.out.println(dist);
		System.out.println(this.fileSystem.getStorage(dist.getDataSegments()[0]
				.getStorageId()));
	}
}
