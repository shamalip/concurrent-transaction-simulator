package sim.application;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sim.common.Utils;

public class ConcurrentSimulatorApp {

	private static final int MPL = 50;

	public static void main(String[] args) throws InterruptedException {
		insertBaselineData();	
		startSimulation();
	}

	private static void startSimulation() throws InterruptedException {
		 ExecutorService executor = Executors.newFixedThreadPool(MPL);

		
        for(int i = 1; i <= 20; i++)
        { 
        	executor.execute(new DBTransactionThread(Utils.readFileInToList("/Users/sadeem/Downloads/project2/data/low_concurrency/t"+(i+7)+".sql")));	
        	
        	executor.execute(new DBTransactionThread(Utils.readFileInToList("/Users/sadeem/Downloads/queries/low_concurrency/qt"+(i+7)+".sql")));	
        	
        	Thread.sleep(300000);
        }
        executor.shutdown();
	}

	private static void insertBaselineData() {
		DBTransaction d = new DBTransaction();
		d.runTransaction(Utils.readFileInToList("/Users/sadeem/Downloads/project2/data/low_concurrency/metadata.sql"));
	}
}
