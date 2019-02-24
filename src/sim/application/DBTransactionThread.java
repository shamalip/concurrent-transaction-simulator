package sim.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import sim.common.AppConfig;

public class DBTransactionThread implements Runnable {

	public DBTransactionThread(String trSet) {
		// TODO Load the transaction set recieved. (could be file name or object)
		// e.g. TransactionBuilder.getTransactionSet(1)
	}

	@Override
	public void run() {
		runTransaction();
	}

	private void runTransaction() {
		Connection conn = connect();
		try {			
			conn.setAutoCommit(false);
			// TODO Execute the transaction set.
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
