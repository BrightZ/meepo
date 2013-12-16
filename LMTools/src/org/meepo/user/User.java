package org.meepo.user;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.meepo.common.CommonUtil;
import org.meepo.config.Environment;
import org.meepo.user.UGRelation.Relation;

/**
 * 
 * @author msmummy
 * 
 */
public class User {

	protected User(String email) {
		this.email = email;
	}

	/**
	 * @param aEmail
	 * @param aPassword
	 * @param aDomain
	 */
	protected User(String aEmail, String aPasswordMd5Salt, Integer aDomain) {
		this(aEmail);
		this.passwordMd5Salt = aPasswordMd5Salt;
		this.domain = aDomain;

		String[] tmp = this.passwordMd5Salt.split(":");
		pass = tmp[0];
		if (tmp.length > 1) {
			salt = tmp[1];
		}
	}

	/**
	 * Get groups a user has joined
	 * 
	 * @param type
	 *            public or regular
	 * @return
	 */
	public Iterable<Group> getJoinedGroupRelations(Group.Type type) {
		Iterable<UGRelation> ugrs = UGRelationFactory.getInstance()
				.getRelations(this);
		ArrayList<Group> retGroups = new ArrayList<Group>();
		for (UGRelation r : ugrs) {
			if (r.getRelation() == Relation.NONE || r.getRelation() == null)
				continue;
			Group group = r.getGroup();
			if (group.getType() == type
					&& (Environment.cross || group.isLocal())) {
				retGroups.add(group);
			}
		}
		return retGroups;
	}

	public UGRelation relationOf(Group g) {
		return UGRelationFactory.getInstance().getRelation(this, g);
	}

	public boolean isAdminOfGroup(Group g) {
		UGRelation ugr = this.relationOf(g);
		return ugr.isAdmin();
	}

	public String getPasswordMd5() {
		return this.passwordMd5Salt;
	}

	public void setDomain(int aDomain) {
		this.domain = aDomain;
	}

	public int getDomain() {
		return this.domain;
	}

	public String getEmail() {
		return this.email;
	}

	public boolean isLocal() {
		return (this.domain.equals(Environment.getDomain()));
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (this == o) {
			return true;
		}
		if (!(o instanceof User)) {
			return false;
		}
		User aUser = (User) o;
		return aUser.getEmail().equalsIgnoreCase(this.email);
	}

	@Override
	public int hashCode() {
		return this.email.toLowerCase().hashCode();
	}

	public boolean checkPassword(String aPassword) {
		if (aPassword == null) {
			return false;
		}
		String aPass = CommonUtil.MD5(aPassword + salt);
		if (pass.equalsIgnoreCase(aPass)) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isRoot() {
		if (this.email.equalsIgnoreCase("msmummy@gmail.com")) {
			return true;
		} else if (this.email.equalsIgnoreCase("meeposearch99713@meepo.org")) {
			return true;
		} else {
			return false;
		}
	}

	public Token getWebToken() {
		if (this.webToken != null) {
			// validate the token
			this.webToken = TokenFactory.getInstance().getToken(
					webToken.getTokenString());
			if (this.webToken == null) {
				this.webToken = TokenFactory.getInstance().genToken(this);
				// logger.error("validate token error fixed!");
			}
		} else {
			this.webToken = TokenFactory.getInstance().genToken(this);
		}
		return this.webToken;
	}

	private String email = "";
	private String passwordMd5Salt = "";

	private String pass;
	private String salt = "";

	private Integer domain = -1;

	private Token webToken = null;
	protected static Logger logger = Logger.getLogger(User.class);
}
