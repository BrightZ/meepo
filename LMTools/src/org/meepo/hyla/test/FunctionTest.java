package org.meepo.hyla.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class FunctionTest extends TestSuite {
	public static Test getTest() {
		TestSuite testSuite = new TestSuite("Function Test");
		testSuite.addTestSuite(GeneralTestCase.class);
		return testSuite;
	}

	public static void main(String[] args) {
		TestRunner.run(getTest());
	}
}
