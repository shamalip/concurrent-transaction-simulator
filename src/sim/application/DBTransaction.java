package sim.application;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sim.common.AppConfig;

public class DBTransaction{
	
	public void runTransaction(List<String> statements) {
		Connection conn = connect();
		try {			
			conn.setAutoCommit(false);
			List<ResultSet> resultList = new ArrayList<>();
			for(String statement: statements) {
				CallableStatement st = conn.prepareCall(statement);
				ResultSet result = st.executeQuery();
				resultList.add(result);
				// TODO log the result and time taken.
			}			
			conn.commit();
		} catch (SQLException e) {
			System.out.println("Rolling back due to : "+ e.getMessage());
			try {
				if (conn != null) {
					conn.rollback();
				}
			} catch (SQLException er) {
				System.out.println(er.getMessage());
			}
		}
		finally {
			close(conn);		
		}
	}

	private void close(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private Connection connect() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(AppConfig.get("DB_CONNECTION"), AppConfig.get("DB_USER"), AppConfig.get("DB_PASSWORD"));
			System.out.println("Connected to the database successfully.");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}	 
		return conn;
	}
}
