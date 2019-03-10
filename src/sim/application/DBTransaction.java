package sim.application;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

import sim.common.AppConfig;

public class DBTransaction{

	protected String threadName = null;
	static int throughput = 0;
	static long startTime = System.currentTimeMillis();
	static long elapsedTime = 0L;
	static double avgTranSize = 0;
	private static long avgQueryTime = 0;
	private static long queriesNum = 0;
	public void runTransaction(List<String> statements) throws IOException {
		Connection conn = connect();
		try {		
			conn.setAutoCommit(false);
			long qStartTime = 0, qEndTime = 0, locAvgQueryTime = 0; 
			boolean selectQuery = false;
			for(String statement: statements) 
			{
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
			synchronized (this) 
			{
					throughput++;
					avgTranSize+= statements.size();
				if(selectQuery)
				{
					queriesNum += statements.size();
					avgQueryTime += (locAvgQueryTime / 1000000);
				}
			}

		} 
		catch (SQLException e) {
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

	public static int getThroughput() {
		return throughput;
	}

	public static double getAvgTranSize() {
		return avgTranSize;
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
			//should open another connection to mysql and run both at the same time
			conn = DriverManager.getConnection(AppConfig.get("DB_CONNECTION"), AppConfig.get("DB_USER"), AppConfig.get("DB_PASSWORD"));
		} 
		catch (SQLException e) 
		{
			System.out.println(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}	 
		return conn;
	}

	public static long getAvgQueryTime() {
		return avgQueryTime;
	}

	public static long getQueriesNum() {
		return queriesNum;
	}
	
}
