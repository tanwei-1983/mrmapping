import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MRMapping {

	static final String DRIVER = "org.postgresql.Driver";
	static final String URL = "jdbc:postgresql://127.0.0.1:5432/foo";
	static final String USER = "postgres";
	static final String PASSWORD = "password";

	public Connection connection = null;

	private void setConnection(boolean beginTran) {
		try {
			Class.forName(DRIVER);
			connection = DriverManager.getConnection(URL, USER, PASSWORD);
			if (beginTran) {
				connection.setAutoCommit(false);
			} else {
				connection.setAutoCommit(true);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			procDBException(e);
			System.exit(0);
		}
	}
	
	// set beginTran to true when you use long connection
	public MRMapping(boolean beginTran) {
		setConnection(beginTran);

	}
	
	public MRMapping(boolean beginTran, Connection existConn) {
		if(existConn!=null){
			this.connection=existConn;
			return;
		}
		setConnection(beginTran);
	}

	public static String implode(String[] array, String glue) {
		StringBuilder sql = new StringBuilder();
		for (String str : array) {
			sql.append(str + glue);
		}
		return sql.substring(0, sql.length() - glue.length());
	}

	public static String getInsertSql(String tbName, List<Map<String, Object>> list) {
		Set<String> keySet = list.get(0).keySet();
		String[] colNameArr = new String[keySet.size()];
		keySet.toArray(colNameArr);
		String colNameStr = " (" + implode(colNameArr, ",") + " )";
		String quesMarkStr;

		String[] quesMarkArr = new String[colNameArr.length];
		Arrays.fill(quesMarkArr, "?");
		if (list.size() == 1) {
			quesMarkStr = " (" + implode(quesMarkArr, ",") + " )";
		} else {
			String[] array = new String[list.size()];
			for (int i = 0; i < list.size(); ++i) {
				array[i] = " (" + implode(quesMarkArr, ",") + " )";
			}
			quesMarkStr = implode(array, ",");
		}
		String insSql = "insert into " + tbName + colNameStr + " values " + quesMarkStr;
		// System.out.println("insSql:"+insSql);
		return insSql;
	}

	// long connection
	public long insertL(String tbName, List<Map<String, Object>> list, boolean getAutoIncId) throws SQLException {
		return psInsert(tbName, list, getAutoIncId);
	}

	// short connection
	public long insertS(String tbName, List<Map<String, Object>> list, boolean getAutoIncId) {
		long retLong = -1;
		try {
			retLong = insertL(tbName, list, getAutoIncId);
			if (connection.getAutoCommit()) {
				close();
			}
		} catch (SQLException e) {
			procDBException(e);
			e.printStackTrace();
		}
		return retLong;
	}


	// long connection
	public void freeOperL(String sql, List<Object> list) throws SQLException {

		PreparedStatement ps = connection.prepareStatement(sql);
		if (list != null) {
			int i = 1;
			for (Object obj : list) {
				ps.setObject(i++, obj);
			}
		}
		ps.executeUpdate();
		ps.close();
	}

	// short connection
	public void freeOperS(String sql, List<Object> list) {
		try {
			freeOperL(sql, list);
			if (connection.getAutoCommit()) {
				close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			procDBException(e);
		}
	}

	public long psInsert(String tbName, List<Map<String, Object>> list, boolean getAutoIncId) throws SQLException {
		long retLong = -1;
		PreparedStatement ps;
		if (list.size() == 1) {
			int i = 1;
			Map<String, Object> map = list.get(0);
			if (getAutoIncId)
				ps = connection.prepareStatement(getInsertSql(tbName, list), Statement.RETURN_GENERATED_KEYS);
			else
				ps = connection.prepareStatement(getInsertSql(tbName, list));
			for (Object obj : map.values()) {
				ps.setObject(i++, obj);
			}
			ps.executeUpdate();

			if (getAutoIncId) {
				ResultSet rs = ps.getGeneratedKeys();
				if (rs.next()) {
					retLong = rs.getLong(1);
				}
				rs.close();
			}
		} else {
			int j = 1;
			ps = connection.prepareStatement(getInsertSql(tbName, list));
			for (Map<String, Object> map : list) {
				for (Object obj : map.values()) {
					ps.setObject(j++, obj);
				}
			}
			ps.executeUpdate();
		}

		ps.close();
		return retLong;
	}

	// long connection
	protected List<Map<String, Object>> selectL(String sql, List<Object> valList) throws SQLException {

		List<Map<String, Object>> resList = new ArrayList<>();
		PreparedStatement ps = connection.prepareStatement(sql);
		// cursor read code:
		// make sure autocommit is off
		// connection.setAutoCommit(false);
		// PreparedStatement cursorPS = connection.prepareStatement(sql,
		// ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		// Turn use of the cursor on.
		// cursorPS.setFetchSize(50);

		int i = 1;
		if (valList != null) {
			for (Object obj : valList) {
				ps.setObject(i++, obj);
			}
		}
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			Map<String, Object> map = new HashMap<>();
			for (int j = 1; j <= rsmd.getColumnCount(); ++j) {
				Object obj = rs.getObject(j);
				String colName = rsmd.getColumnLabel(j);
				map.put(colName, obj);
			}
			resList.add(map);
		}
		rs.close();
		ps.close();
		return resList;
	}

	// short connection
	protected List<Map<String, Object>> selectS(String sql, List<Object> valList) {
		List<Map<String, Object>> resList = null;
		try {
			resList = selectL(sql, valList);
			if (connection.getAutoCommit()) {
				close();
			}
		} catch (SQLException e) {
			procDBException(e);
			e.printStackTrace();
		}

		return resList;
	}

	// it only can be used in short connection
	public void procDBException(Exception e) {
		try {
			if (connection != null) {
				boolean inTransaction = !connection.getAutoCommit();

				if (inTransaction) { // in Transaction
					connection.rollback();
				}
				close();
			}

		} catch (SQLException se) {
			se.printStackTrace();
		}

	}

	// short connection commit
	public void commitS() {
		try {
			connection.commit();
			close();
		} catch (SQLException e) {
			procDBException(e);
			e.printStackTrace();
		}

	}
	public void commitS(Connection conn) {
		if(conn!=null){
			return;
		}
		try {
			connection.commit();
			close();
		} catch (SQLException e) {
			procDBException(e);
			e.printStackTrace();
		}

	}

	// long connection commit
	public void commitL(boolean isClose) throws SQLException {
		connection.commit();
		if (isClose) {
			close();
		}
	}

	public void close() throws SQLException {
		if (connection != null) {
			connection.close();
			connection = null;
		}
	}

	public static String getSetStatement(Map<String, Object> map, List<Object> valList) {
		StringBuilder sb = new StringBuilder(" set ");
		for (String key : map.keySet()) {
			sb.append(key + "=?,");
			valList.add(map.get(key));
		}
		return sb.substring(0, sb.length() - 1);
	}

	public static String getWhereStatement(Map<String, Object> map, List<Object> objList) {
		StringBuilder sb = new StringBuilder(" where ");
		String andStr=" and ";
		for (String key : map.keySet()) {
			Object valObj = map.get(key);
			if (valObj instanceof List) {
				objList.addAll((List) valObj);
			} else {
				objList.add(valObj);
			}
			sb.append(key + andStr);
		}

		return sb.substring(0, sb.length() - andStr.length());
	}
	
	//inOrNotInTranTest1 can be wrapped by other transactions or begin a transaction itself
    static void inOrNotInTranTest1(String test, Connection conn){
    	MRMapping db=new MRMapping(true, conn);
    	List<Object>list=new ArrayList<>();
    	db.freeOperS("", list);
    	db.freeOperS("", list);
    	db.commitS(conn);
    }
    
    static void inOrNotInTranTest2(){ 
    	MRMapping db=new MRMapping(true);
    	inOrNotInTranTest1("", db.connection); 
    	db.commitS();
    	
    	inOrNotInTranTest1("", null); 
    }
    
}
