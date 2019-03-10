package sim.application;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import sim.common.AppConfig;

public class DBTransaction{

	protected String threadName = null;
	static double totalNumOfTransactions = 0;
	static double totalTranSize = 0;
	private static double totalQueryTime = 0;
	private static double totalNumOfQueries = 0;
	static double totalTransactionTime = 0;
	String query = "";
	public void runTransaction(List<String> statements) throws IOException {
		Connection conn = connect();
		try {		
			long qStartTime = 0, qEndTime = 0, locAvgQueryTime = 0, tStartTime = 0, tEndTime = 0;
			tStartTime = System.currentTimeMillis();
			conn.setAutoCommit(false);
			boolean selectQuery = false;
			for(String statement: statements) 
			{
				query = statement;
				Statement st = conn.createStatement();//ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				qStartTime = System.nanoTime();
				selectQuery = st.execute(statement);	
				qEndTime = System.nanoTime();
				if(selectQuery)
				{
					locAvgQueryTime += (qEndTime - qStartTime);
				}
				qEndTime = 0;
				qStartTime = 0;
			}			
			conn.commit();
			tEndTime = System.currentTimeMillis();
			synchronized (this) 
			{
				totalNumOfTransactions++;
					totalTranSize+= statements.size();
					totalTransactionTime+= (tEndTime - tStartTime);
				if(selectQuery)
				{
					totalNumOfQueries += statements.size();
					totalQueryTime += (locAvgQueryTime / 1000000);
				}
			}

		} 
		catch (SQLException e) {
			System.out.println(query);
			System.out.println("Rolling back in "+ threadName + " due to : "+ e.getMessage());
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

	public static double getTotalTransactionTime() {
		return totalTransactionTime;
	}

	public static double getTotalNumOfTransactions() {
		return totalNumOfTransactions;
	}

	public static double getTotalTranSize() {
		return totalTranSize;
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
		try
		{
			conn = DriverManager.getConnection(AppConfig.get("DB_CONNECTION1"), AppConfig.get("DB_USER"), AppConfig.get("DB_PASSWORD"));
		} 
		catch (SQLException e) 
		{
			System.out.println(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}	 
		return conn;
	}

	public static double getTotalQueryTime() {
		return totalQueryTime;
	}

	public static double getTotalNumOfQueries() {
		return totalNumOfQueries;
	}
	
}
