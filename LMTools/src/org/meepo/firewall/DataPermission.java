package org.meepo.firewall;

public class DataPermission {

	// Old_TODO : more details about exception
	public DataPermission(int pNum) throws Exception {
		this.dpNum = pNum;
		this.dpStr = "";

		for (int i = 0; i < PERMISSION_SIZE; i++) {
			if ((dpNum >> i & 1) == 1) {
				dpStr = dpStr + PERMISSION_CHAR[i];
			} else {
				dpStr = dpStr + PERMISSION_NOT;
			}
		}
		if (pNum > ((1 << PERMISSION_SIZE) - 1) || pNum < 0) {
			throw new Exception("DataPermission pNum is not valid");
		} else {
			this.isValid = true;
		}
	}

	// Old_TODO : more details about exception
	public DataPermission(String pStr) throws Exception {
		this.dpStr = pStr;
		this.dpNum = 0;

		if (pStr.length() > PERMISSION_SIZE) {
			throw new Exception("DataPermission pStr is not valid");
		}

		boolean flag;
		for (int i = 0; i < pStr.length(); i++) {
			flag = false;
			for (int j = 0; j < PERMISSION_SIZE && !flag; j++) {
				if (pStr.charAt(i) == PERMISSION_CHAR[j].charAt(0)) {
					flag = true;
					this.dpNum += (1 << j);
				}
			}
			if (pStr.charAt(i) == PERMISSION_NOT.charAt(0)) {
				flag = true;
			}
			if (!flag) {
				throw new Exception("DataPermission pStr is not valid");
			}
		}
		this.isValid = true;
	}

	public int getDPNum() {
		return this.dpNum;
	}

	public String getDPStr() {
		return this.dpStr;
	}

	public boolean isValid() {
		return this.isValid;
	}

	private String dpStr = "";// data permission string
	private int dpNum;// data permission number
	private boolean isValid = false;
	public static final int PERMISSION_SIZE = 5;
	public static final String[] PERMISSION_CHAR = { "r", "w", "d", "m", "s" };
	public static final String PERMISSION_NOT = "-";

	// RWDMS : read, write, delete, map, set replication number
	// 1 2 4 8 16

	public static void main(String args[]) {
		int pNum = 1 * 1 + 2 * 1 + 4 * 1 + 8 * 1 + 16 * 1;
		String pStr = "---x-";
		try {
			// DataPermission dp = new DataPermission(pNum);
			DataPermission dp = new DataPermission(pStr);
			System.out.println(dp.getDPNum() + "\t" + dp.getDPStr() + "\t"
					+ dp.isValid());
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}
}
