package org.meepo.test.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestInputEncoding {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("中文测试开始");
		BufferedReader stdin = new BufferedReader(new InputStreamReader(
				System.in));
		String s = stdin.readLine();
		System.out.println(s);

	}

}
