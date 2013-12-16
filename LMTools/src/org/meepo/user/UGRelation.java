package org.meepo.user;

public class UGRelation {

	public UGRelation(User aUser, Group aGroup, Relation aRelation) {
		this.user = aUser;
		this.group = aGroup;
		this.relation = aRelation;
	}

	public enum Relation {
		NONE, ADMIN, MEMBER,
	}

	public Relation getRelation() {
		return relation;
	}

	public void setRelation(Relation relation) {
		this.relation = relation;
	}

	public User getUser() {
		return user;
	}

	public Group getGroup() {
		return group;
	}

	public boolean isAdmin() {
		return (relation != null && relation == Relation.ADMIN);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (this == o) {
			return true;
		}
		if (!(o instanceof UGRelation)) {
			return false;
		}
		UGRelation aRelation = (UGRelation) o;

		return aRelation.getUser().equals(this.user)
				&& aRelation.getGroup().equals(this.group);
	}

	@Override
	public int hashCode() {
		return this.user.hashCode() + this.group.hashCode();
	}

	private User user;
	private Group group;
	private Relation relation;
}
