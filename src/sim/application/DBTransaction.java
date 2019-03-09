package sim.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import sim.common.AppConfig;

public class DBTransaction{

	protected String threadName = null;
	static int throughput = 0;
	//check query time
	public void runTransaction(List<String> statements) {
		Connection conn = connect();
		try {		
			System.out.println("Transaction" + threadName + " started");
			conn.setAutoCommit(false);
			long qStartTime = 0, qEndTime = 0, avgQueryTime = 0, queriesNum = statements.size();
			boolean selectQuery = false;
			for(String statement: statements) 
			{
				Statement st = conn.createStatement();//ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				qStartTime = System.nanoTime();
				selectQuery = st.execute(statement);	
				qEndTime = System.nanoTime();
				if(selectQuery)
				{
					//System.out.println("Size of results " + st.getResultSet().last());
					avgQueryTime += (qEndTime - qStartTime);
				}
				qEndTime = 0;
				qStartTime = 0;
			}			
			conn.commit();
			synchronized (this) 
			{
				throughput++;
		        //Files.write(Paths.get("/Users/sadeem/Downloads/project2/data/low_concurrency/"+threadName+".csv"), sb.toString().getBytes());
			}
			System.out.println("Transaction " + threadName + " finished");
			if(selectQuery)
			{
				avgQueryTime = avgQueryTime / 1000000;//measure time in milliseconds
				System.out.println(threadName+" average query time: " + avgQueryTime / queriesNum);
			}
			System.out.println("Throughput: " + throughput);
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
}
