package com.tongtech.cj.dbMeta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.caijun.utils.str.StringUtil;
import com.caijun.utils.xml.XMLException;
import com.caijun.utils.xml.XMLHandler;

public class TBMeta {
	private static final String line = System.getProperty("line.separator");
	private String name;// ������
	private List<ColMeta> cols;// ���е���
	private List<ColMeta> pks;// ���������

	public TBMeta(String schema, String name, DataSource ds) throws DBMetaException {
		schema = StringUtil.toUpperCase(StringUtil.trimDown(schema));
		this.name = StringUtil.toLowerCase(StringUtil.trimDown(name));
		pks = new ArrayList<ColMeta>();
		Connection conn = null;
		try {
			conn = ds.getConnection();
			String realTBName = getTableRealName(schema, StringUtil.toUpperCase(this.name), conn);
			if (realTBName == null) {
				throw new DBMetaException("��[" + schema + "." + name + "]������.");
			}
			DatabaseMetaData dbmd = conn.getMetaData();
			cols = getColsFromDataSource(dbmd, schema, realTBName);
			for (ColMeta col : cols) {
				if (col.ispk) {
					pks.add(col);
				}
			}
		} catch (SQLException e) {
			throw new DBMetaException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	public TBMeta(String xml) throws DBMetaException {
		try {
			cols = new ArrayList<ColMeta>();
			pks = new ArrayList<ColMeta>();
			Document doc = com.caijun.utils.xml.XMLHandler.loadXMLString(xml);
			Node table = XMLHandler.getSubNode(doc, "t");
			name = StringUtil.toLowerCase(StringUtil.trimDown(XMLHandler.getTagValue(table, "n")));
			List<Node> colNodes = XMLHandler.getNodes(XMLHandler.getSubNode(table, "cs"), "c");
			for (Iterator<Node> iterator = colNodes.iterator(); iterator.hasNext();) {
				Node col = iterator.next();

				String size = XMLHandler.getTagValue(col, "s");
				String digits = XMLHandler.getTagValue(col, "d");
				String pk = XMLHandler.getTagAttribute(col, "pk");
				String fk = XMLHandler.getTagAttribute(col, "fk");
				String nul = XMLHandler.getTagAttribute(col, "nvl");
				if (pk == null) {
					pk = "false";
				}
				if (fk == null) {
					fk = "false";
				}
				if (nul == null) {
					nul = "true";
				}
				ColMeta colMeta = new ColMeta(XMLHandler.getTagValue(col, "n"), XMLHandler.getTagValue(col, "t"),
						size == null ? 0 : Integer.parseInt(size), digits == null ? 0 : Integer.parseInt(digits),
						Boolean.parseBoolean(nul), Boolean.parseBoolean(pk));
				cols.add(colMeta);
				if (colMeta.ispk) {
					pks.add(colMeta);
				}
			}
		} catch (XMLException e) {
			throw new DBMetaException(e);
		}
	}

	/**
	 * ��ñ���
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * ���������
	 * 
	 * @return
	 */
	public List<ColMeta> getCols() {
		return new ArrayList<ColMeta>(cols);
	}

	/**
	 * ���������
	 * 
	 * @return
	 */
	public List<ColMeta> getPKCols() {
		return new ArrayList<ColMeta>(pks);
	}

	/**
	 * ����XML��ʽ
	 * 
	 * @return
	 */
	public String toXML() {
		StringBuffer sb = new StringBuffer("<t>").append(line);
		sb.append("<n>").append(this.name).append("</n>").append(line);
		sb.append("<cs>").append(line);
		for (ColMeta col : cols) {
			sb.append("<c");
			if (col.ispk) {
				sb.append(" pk='true' ");
			}
			if (col.isfk) {
				sb.append(" fk='true' ");
			}
			if (!col.nul) {
				sb.append(" nvl='false' ");
			}
			sb.append("><n>").append(col.getName()).append("</n><t>").append(col.getType()).append("</t><s>")
					.append(col.getSize()).append("</s><d>").append(col.getDigits()).append("</d></c>" + line);
		}
		sb.append("</cs>").append(line);
		sb.append("</t>");
		return sb.toString();
	}

	/**
	 * ��Ĵ���������PK,FP��
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String createDDLSQL(String schema) {
		String sql1 = createTableSQL(schema);
		String sql2 = createPKSQL(schema);
		String sql3 = createFKSQL(schema);
		return sql1 + line + sql2 + line + sql3 + line;
	}

	/**
	 * �������
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String createTableSQL(String schema) {
		StringBuffer sql = new StringBuffer("create table ").append(schema == null ? "" : schema + ".")
				.append(this.name).append(" (").append(line);
		for (ColMeta col : cols) {
			// sql.append(" ").append(col.getName()).append(" ")
			// .append(metaEngine.getColTypeOfDB(col));
			if (!col.nul) {
				sql.append(" not null");
			}
			sql.append(",").append(line);
		}
		sql.deleteCharAt(sql.length() - line.length() - 1);
		sql.append(");");
		return sql.toString();
	}

	/**
	 * ������ɾ�����
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String dropTableSQL(String schema) {
		StringBuffer sql = new StringBuffer("drop table ").append(schema == null ? "" : schema + ".").append(name)
				.append(";");
		return sql.toString();
	}

	/**
	 * �������������
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String createPKSQL(String schema) {
		if (pks.size() == 0) {
			return "";
		}
		StringBuffer sql = new StringBuffer("alter table ").append(schema == null ? "" : schema + ".").append(name)
				.append(" add constraint ").append(name).append("_PK primary key (");
		for (ColMeta pk : pks) {
			sql.append(pk.name).append(",");
		}
		sql.deleteCharAt(sql.length() - 1);
		sql.append(");");
		return sql.toString();
	}

	/**
	 * ɾ�����������
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String dropPKSQL(String schema) {
		StringBuffer sql = new StringBuffer();

		return sql.toString();
	}

	/**
	 * ������������
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String createFKSQL(String schema) {
		StringBuffer sql = new StringBuffer();

		return sql.toString();
	}

	/**
	 * ɾ����������
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String dropFKSQL(String schema) {
		StringBuffer sql = new StringBuffer();

		return sql.toString();
	}

	/**
	 * ���������ݵ�������
	 * 
	 * @param schema
	 *            ��Ϊ��
	 * @return
	 */
	public String cleanDatasSQL(String schema) {
		return "truncate table " + schema == null ? "" : schema + "." + this.name + ";";
	}

	static public class ColMeta {
		private String name;// ������
		private String type;// ������
		private int size;// ��С
		private int digits;// ����
		private boolean nul;// �Ƿ�����Ϊ��
		private boolean ispk;// �Ƿ�������
		private boolean isfk;// �Ƿ������

		public ColMeta(String name, String type, int size, int digits, boolean nul, boolean ispk) {
			super();
			this.name = StringUtil.toLowerCase(StringUtil.trimDown(name));
			this.type = StringUtil.toUpperCase(StringUtil.trimDown(type));
			this.size = size;
			this.digits = digits;
			this.nul = nul;
			this.ispk = ispk;
		}

		public String toXML() {
			StringBuffer sb = new StringBuffer("<c");
			if (ispk) {
				sb.append(" pk='true' ");
			}
			if (isfk) {
				sb.append(" fk='true' ");
			}
			if (!nul) {
				sb.append(" nvl='false' ");
			}
			sb.append("><n>").append(this.name).append("</n><t>").append(this.type).append("</t><s>").append(this.size)
					.append("</s><d>").append(this.digits).append("</d></c>");
			return sb.toString();
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			if (this.size > 0) {
				sb.append(this.name).append("   ").append(this.type.replaceAll("\\(.*\\)", ""));
				sb.append("(").append(this.size);
				if (this.digits > 0) {
					sb.append("," + this.digits);
				}
				sb.append(")");
			} else {
				sb.append(this.name).append("   ").append(this.type);
			}
			if (!this.nul) {
				sb.append(" not null");
			}
			if (this.ispk) {
				sb.append("  pk");
			}
			if (this.isfk) {
				sb.append("  fk");
			}
			return sb.toString();
		}

		public String getName() {
			return name;
		}

		public String getType() {
			return type;
		}

		public int getSize() {
			return size;
		}

		public int getDigits() {
			return digits;
		}

		public boolean isIspk() {
			return ispk;
		}

		public boolean isIsfk() {
			return isfk;
		}
	}

	private Set<String> getPKColFromDataSource(DatabaseMetaData dbmd, String schema, String tbName)
			throws SQLException {
		Set<String> set = new HashSet<String>();
		ResultSet rs = null;
		try {
			rs = dbmd.getPrimaryKeys(null, schema, tbName);
			while (rs.next()) {
				set.add(StringUtil.toLowerCase(rs.getString("COLUMN_NAME")));
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
		}
		return set;
	}

	private String getTableRealName(String schema, String tbName, Connection conn) throws SQLException {
		String sql = "select table_name from all_tables where owner='" + schema + "' and NLS_UPPER(table_name)='"
				+ tbName + "'";
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

	private List<ColMeta> getColsFromDataSource(DatabaseMetaData dbmd, String schema, String tbName)
			throws SQLException {
		List<ColMeta> cols = new ArrayList<ColMeta>();
		ResultSet rs = null;
		try {

			Set<String> pkSet = getPKColFromDataSource(dbmd, schema, tbName);

			rs = dbmd.getColumns(null, schema, tbName, null);
			while (rs.next()) {
				String name = StringUtil.toLowerCase(rs.getString("COLUMN_NAME"));
				ColMeta colMeta = new ColMeta(name, rs.getString("TYPE_NAME"), rs.getInt("COLUMN_SIZE"),
						rs.getInt("DECIMAL_DIGITS"), rs.getBoolean("NULLABLE"), pkSet.contains(name));
				cols.add(colMeta);
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			rs.close();
		}
		return cols;
	}
}
