package org.meepo.xmlrpc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.meepo.hyla.FileObject;
import org.meepo.hyla.util.FileSystemUtils;

public class Ugly {

	public String withoutTrashPrefix(String path) {
		String ret;
		if (!path.startsWith("/.Trash")) {
			return path;
		}

		ret = path.split("/.Trash")[1];
		return ret;
	}

	public boolean checkIsTime(String str) {
		long l_time;
		Date cas, start, end;
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		;

		try {
			l_time = Long.parseLong(str);
		} catch (NumberFormatException e) {
			return false;
		}

		try {
			cas = new Date(l_time);
			start = f.parse("2011-01-01");
		} catch (ParseException e) {
			return false;
		}

		end = new Date();
		if (cas.after(start) && cas.before(end)) {
			return true;
		}
		return false;
	}

	// Deleted files are all the leaf of /.Trash directory tree.
	// Leaf's name contains time string.
	public boolean checkIsLeaf(String path) {
		FileObject obj1 = assist.getHylaFileSystem().openObject(path);
		String name = obj1.getName();

		String s_time = name.split("\\.")[0];
		return checkIsTime(s_time);
	}

	public String withoutTimeMidFix(String path) {
		String[] segs = FileSystemUtils.splitPath(path);
		String ret = "";
		String s_time;

		for (int i = 0; i < segs.length; i++) {
			s_time = segs[i].split("\\.")[0];
			if (s_time.length() == segs[i].length()) {
				ret += "/" + segs[i];
				continue;
			}

			if (checkIsTime(s_time)) {
				ret += "/"
						+ segs[i].substring(s_time.length() + 1,
								segs[i].length());
			} else {
				ret += "/" + segs[i];
			}
		}

		if (ret.charAt(0) != '/')
			ret = "/" + ret;
		return ret;

		/*
		 * FileObject obj1 = assist.getHylaFileSystem().openObject(path); String
		 * name = obj1.getName(); String pathall = obj1.getPath();
		 * 
		 * // String name = "1349605772021.asdf.txt"; // String pathall =
		 * "/myspace/1349605772021.asdf.txt";
		 * 
		 * String ret = path.substring(0, pathall.length() - name.length());
		 * String s_time = name.split("\\.")[0]; // check if is time boolean
		 * isTime = checkIsTime(s_time);
		 * 
		 * if(isTime){ ret = ret + name.substring(s_time.length()+1,
		 * name.length()); return ret; }
		 * 
		 * return path;
		 */
	}

	public String withoutTTFix(String path) {
		String ret;
		ret = withoutTrashPrefix(path);
		ret = withoutTimeMidFix(ret);
		return ret;
	}

	public void areyoukiddingme(String path) {
		String src_path;
		String dst_path;
		String par_path;
		FileObject src_obj;
		FileObject dst_obj;
		FileObject par_obj;
		FileObject src_sons[];
		FileObject dst_sons[];

		src_path = path;
		dst_path = Ugly.getInstance().withoutTTFix(src_path);
		src_obj = assist.getHylaFileSystem().openObject(src_path);
		dst_obj = assist.getHylaFileSystem().openObject(dst_path);

		if (checkIsLeaf(path)) {
			try {
				if (dst_obj.exists() && dst_obj.isDirectory()) {
					src_sons = src_obj.list();

					for (int i = 0; i < src_sons.length; i++) {
						areyoukiddingme(src_sons[i].getRealPath());
					}

				} else if (dst_obj.exists() && dst_obj.isFile()) {
					String new_name = src_obj.getName();
					String new_path = dst_obj.getParent().getRealPath() + "/"
							+ new_name;
					FileObject new_obj = assist.getHylaFileSystem().openObject(
							new_path);
					par_path = new_obj.getParent().getPath();
					par_obj = assist.getHylaFileSystem().openObject(new_path);
					if (!par_obj.exists()) {
						par_obj.makeDirectory();
					}
					src_obj.moveTo(new_obj);
					logger.info(String.format("restored src:%s. dst:%s.",
							src_path, new_path));
				} else {
					par_path = dst_obj.getParent().getPath();
					par_obj = assist.getHylaFileSystem().openObject(par_path);
					if (!par_obj.exists()) {
						par_obj.makeDirectory();
					}
					src_obj.moveTo(dst_obj);
					logger.info(String.format("restored src:%s. dst:%s.",
							src_path, dst_path));
				}
			} catch (Exception e) {
				logger.error(String.format("restore src:%s. dst:%s. failed",
						src_path, dst_path), e);
				return;
			}
		} else {
			try {
				if (dst_obj.exists() && dst_obj.isDirectory()) {
					src_sons = src_obj.list();

					for (int i = 0; i < src_sons.length; i++) {
						areyoukiddingme(src_sons[i].getRealPath());
					}

				} else if (dst_obj.exists() && dst_obj.isFile()) {
					String new_name = src_obj.getName();
					String new_path = dst_obj.getParent().getRealPath() + "/"
							+ new_name;
					FileObject new_obj = assist.getHylaFileSystem().openObject(
							new_path);
					par_path = new_obj.getParent().getPath();
					par_obj = assist.getHylaFileSystem().openObject(new_path);
					if (!par_obj.exists()) {
						par_obj.makeDirectory();
					}
					src_obj.moveTo(new_obj);
					logger.info(String.format("restored src:%s. dst:%s.",
							src_path, new_path));
				} else {
					par_path = dst_obj.getParent().getPath();
					par_obj = assist.getHylaFileSystem().openObject(par_path);
					if (!par_obj.exists()) {
						par_obj.makeDirectory();
					}
					src_obj.moveTo(dst_obj);
					logger.info(String.format("restored src:%s. dst:%s.",
							src_path, dst_path));
				}
			} catch (Exception e) {
				logger.error(String.format("restore src:%s. dst:%s. failed",
						src_path, dst_path), e);
				return;
			}

		}

	}

	/*
	 * //path shoud be realPathString. //restore file recursely public void
	 * areyoukiddingme(String path){ String src_path; String dst_path; String
	 * par_path; FileObject src_obj; FileObject dst_obj; FileObject par_obj;
	 * FileObject src_sons[]; FileObject dst_sons[];
	 * 
	 * src_path = path; dst_path = Ugly.getInstance().withoutTTFix(src_path);
	 * src_obj = assist.getHylaFileSystem().openObject(src_path); dst_obj =
	 * assist.getHylaFileSystem().openObject(dst_path);
	 * 
	 * try { if(dst_obj.exists() && dst_obj.isDirectory()){ src_sons =
	 * src_obj.list();
	 * 
	 * for(int i = 0;i < src_sons.length ;i ++){
	 * areyoukiddingme(src_sons[i].getRealPath()); }
	 * 
	 * } else if(dst_obj.exists() && dst_obj.isFile()){ String new_name =
	 * src_obj.getName(); String new_path = dst_obj.getParent().getRealPath() +
	 * "/" + new_name; FileObject new_obj =
	 * assist.getHylaFileSystem().openObject(new_path); par_path =
	 * new_obj.getParent().getPath(); par_obj =
	 * assist.getHylaFileSystem().openObject(new_path); if(!par_obj.exists()){
	 * par_obj.makeDirectory(); } src_obj.moveTo(new_obj);
	 * logger.error(String.format("restored src:%s. dst:%s.",src_path,
	 * new_path)); } else { par_path = dst_obj.getParent().getPath(); par_obj =
	 * assist.getHylaFileSystem().openObject(par_path); if(!par_obj.exists()){
	 * par_obj.makeDirectory(); } src_obj.moveTo(dst_obj);
	 * logger.error(String.format("restored src:%s. dst:%s.", src_path,
	 * dst_path)); }
	 * 
	 * } catch (Exception e){ return ; }
	 * 
	 * }
	 */

	public static void main(String args[]) {
		String path1 = "/.Trash/Groups/testGroup@移动测试/upload/1349788256503.redis-2.4.16.tar.gz";
		String ret1 = Ugly.getInstance().withoutTrashPrefix(path1);
		System.out.println(ret1);

		String path2 = ret1;
		String ret2 = Ugly.getInstance().withoutTimeMidFix(path2);
		System.out.println(ret2);
	}

	public static Ugly getInstance() {
		return instance;
	}

	private static Ugly instance = new Ugly();

	private static MeepoAssist assist = MeepoAssist.getInstance();

	private static Logger logger = Logger.getLogger(Ugly.class);

}
