package org.meepo.hyla.util;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionUtils {
	private static final Class<?>[] EMPTY_CLASS_ARRAY = new Class[] {};
	private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_MAP = new ConcurrentHashMap<Class<?>, Constructor<?>>();

	@SuppressWarnings("unchecked")
	public static <T> T newClassInstance(Class<T> clazz) {
		T result;
		try {
			Constructor<T> method = (Constructor<T>) CONSTRUCTOR_MAP.get(clazz);
			if (method == null) {
				method = clazz.getDeclaredConstructor(EMPTY_CLASS_ARRAY);
				method.setAccessible(true);
				CONSTRUCTOR_MAP.put(clazz, method);
			}
			result = method.newInstance();
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T newClassInstance(String className) {
		T result;
		try {
			Class<T> clazz = (Class<T>) Class.forName(className);
			Constructor<T> method = (Constructor<T>) CONSTRUCTOR_MAP.get(clazz);
			if (method == null) {
				method = clazz.getDeclaredConstructor(EMPTY_CLASS_ARRAY);
				method.setAccessible(true);
				CONSTRUCTOR_MAP.put(clazz, method);
			}
			result = method.newInstance();
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
