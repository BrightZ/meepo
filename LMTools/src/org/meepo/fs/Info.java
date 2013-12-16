package org.meepo.fs;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class Info {
	public Info() {

	}

	public Info(Map<Object, Object> map) {
		for (Object o : map.keySet()) {
			try {
				Field f = this.getClass().getDeclaredField(o.toString());
				f.setAccessible(true);
				f.set(this, map.get(o));
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	public Map<Object, Object> toMap() {
		HashMap<Object, Object> map = new HashMap<Object, Object>();
		Field fields[] = this.getClass().getDeclaredFields();
		try {
			for (Field f : fields) {
				f.setAccessible(true);
				if (Modifier.isStatic(f.getModifiers())) {
					continue;
				}
				map.put(f.getName(), f.get(this));
			}
		} catch (IllegalArgumentException e) {
			// Should not happen
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// Should not happen
			e.printStackTrace();
		}
		return map;
	}
}
