package com.tongtech.cj.dbMeta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.caijun.utils.str.StringUtil;
import com.caijun.utils.xml.XMLException;
import com.caijun.utils.xml.XMLHandler;

public class TrigMeta {
	private static final String line = System.getProperty("line.separator");
	private String name;
	private String content;

	public TrigMeta(String schema, String name, DataSource ds)
			throws DBMetaException {
		this.name = StringUtil.toLowerCase(StringUtil.trimDown(name));
		schema = StringUtil.toUpperCase(StringUtil.trimDown(schema));
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = ds.getConnection();
			String trigName = getTriggerRealName(schema, name, conn);
			if (trigName == null) {
				throw new DBMetaException("´¥·¢Æ÷[" + schema + "." + name
						+ "]²»´æÔÚ.");
			}
			String sql = "select dbms_metadata.get_ddl('TRIGGER','" + trigName
					+ "','" + schema + "') from dual";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				this.content = rs.getString(1);
				this.content = StringUtil.truncate(StringUtil
						.trimDown(StringUtil.cutToIndexOf(this.content,
								"ALTER TRIGGER")), this.content.length() - 2);
			} else {
				this.content = null;
			}
		} catch (SQLException e) {
			throw new DBMetaException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	public TrigMeta(String xml) throws DBMetaException {
		try {
			Document doc = XMLHandler.loadXMLString(xml);
			Node trigger = XMLHandler.getSubNode(doc, "t");
			name = StringUtil.trimDown(XMLHandler.getTagValue(trigger, "n"));
			content = StringUtil.trimDown(XMLHandler.getTagValue(trigger, "c"));
		} catch (XMLException e) {
			throw new DBMetaException(e);
		}
	}

	public String toXML() {
		StringBuffer sb = new StringBuffer("<t>").append(line);
		sb.append("<n>").append(this.name).append("</n>").append(line);
		sb.append("<c><![CDATA[").append(line);
		sb.append(this.content).append(line);
		sb.append("]]></c>").append(line);
		sb.append("</t>");
		return sb.toString();
	}

	public String getName() {
		return name;
	}

	public String getContent() {
		return content;
	}

	private String getTriggerRealName(String schema, String triggerName,
			Connection conn) throws SQLException {
		String sql = "select trigger_name from all_triggers where owner='"
				+ schema + "' and NLS_LOWER(trigger_name)='" + triggerName
				+ "'";
		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery(sql);
			if (rs.next()) {
				return rs.getString(1);
			} else {
				return null;
			}
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e) {
			}
		}
	}

}
