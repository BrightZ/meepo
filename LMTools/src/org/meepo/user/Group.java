package org.meepo.user;

import org.meepo.config.Environment;

public class Group {
	// private long id;

	// private String alias;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Group(String aName, Integer aDomain, Type aType) {
		this.name = aName;
		this.domain = aDomain;
		this.type = aType;
	}

	public Integer getDomain() {
		return domain;
	}

	public boolean isPublic() {
		return (this.type == Type.PUBLIC);
	}

	public boolean isRegular() {
		return (this.type == Type.REGULAR);
	}

	public void setPublic() {
		this.type = Type.PUBLIC;
	}

	public Type getType() {
		return this.type;
	}

	public boolean isLocal() {
		return (this.domain.equals(Environment.getDomain()));
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || !(o instanceof Group)) {
			return false;
		}
		Group g = (Group) o;
		return this.name.equalsIgnoreCase(g.getName());
	}

	@Override
	public int hashCode() {
		return name.toLowerCase().hashCode();
	}

	public enum Type {
		WHATEVER, REGULAR, PUBLIC
	}

	private Type type;
	private String name;
	private Integer domain;
}
