package org.meepo.common;

import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class CommonUtil {
	private static final char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz"
			+ "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();

	/**
	 * generate a random String
	 * 
	 * @param length
	 *            the length of the random string u want
	 * @return
	 */
	public static String randomString(int length) {
		if (length < 1) {
			return null;
		}
		Random randGen = new Random();
		char[] randBuffer = new char[length];
		for (int i = 0; i < randBuffer.length; i++) {
			randBuffer[i] = numbersAndLetters[randGen.nextInt(71)];
		}
		return new String(randBuffer);
	}

	/**
	 * a md5 method, probably not efficient cause we use the String.format()
	 * luckily we don't care about efficiency now
	 * 
	 * @param source
	 * @return 32 characters as a md5 result of source
	 */
	public static String MD5(String source) {
		String result = null;
		byte[] sourceBytes = source.getBytes();
		try {
			java.security.MessageDigest md = java.security.MessageDigest
					.getInstance("MD5");
			md.update(sourceBytes);
			StringBuilder sb = new StringBuilder();
			for (byte b : md.digest()) {
				sb.append(String.format("%02X", b));
			}
			result = sb.toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * a sh1 method, probably not efficient cause we use the String.format()
	 * luckily we don't care about efficiency now
	 * 
	 * @param source
	 * @return 32 characters as a sha1 result of source
	 */
	public static String SHA1(String source) {
		String result = null;
		byte[] sourceBytes = source.getBytes();
		try {
			java.security.MessageDigest md = java.security.MessageDigest
					.getInstance("SHA1");
			md.update(sourceBytes);
			StringBuilder sb = new StringBuilder();
			for (byte b : md.digest()) {
				sb.append(String.format("%02X", b));
			}
			result = sb.toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static void main(String[] args) {
		System.out.println(MD5("huaishaQBav2carii2ZTjDf3Dim5OeSuDEVyA1z"));
		// System.out.println(SHA1("abc"));
		// String path = '/' + "00002f8f";
		// String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
		// String test = path + date + "123";
		// System.out.println(SHA1(path + date + "123"));
		// System.out.println(SHA1(test));
		// System.out.println(test);
		// System.out.println(SHA1("/00002f8f20110707123"));
	}
}
