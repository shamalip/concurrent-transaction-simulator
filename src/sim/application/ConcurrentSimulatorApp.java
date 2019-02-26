package sim.application;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sim.common.AppConfig;
import sim.common.Utils;

public class ConcurrentSimulatorApp {

	private static final int MPL = 50;

	public static void main(String[] args) throws InterruptedException {
		insertBaselineData();	
		startSimulation();
	}

	private static void startSimulation() throws InterruptedException {
		 ExecutorService executor = Executors.newFixedThreadPool(MPL);

		/**
		 * Let's have 80% inserts. 20% searches.
		 * 
		 * **/
		int searchUnit = (20 * MPL)/100;
		int semanticInsertUnit = (40 * MPL)/100;
		int observationInsertUnit = (40 * MPL)/100;
		
        for(int i = 1; i <= AppConfig.getInt("TOTAL_TASKS_COUNT"); i++){ 
        	for(int j = i; j < i + semanticInsertUnit; j++) {
        		executor.execute(new DBTransactionThread(Utils.readFileInToList("sem"+i+".sql")));	
        	}
        	i = i + semanticInsertUnit;
        	
        	for(int j = i; j < i + observationInsertUnit; j++) {
        		executor.execute(new DBTransactionThread(Utils.readFileInToList("obs"+i+".sql")));	
        	}
        	i = i + observationInsertUnit; 
        	
        	for(int j = i; j < i + searchUnit; j++) {
        		executor.execute(new DBTransactionThread(Utils.readFileInToList("search"+i+".sql")));	
        	}
        	i = i + searchUnit;    	
        	
        	Thread.sleep(300000);
        }
        executor.shutdown();
	}

	private static void insertBaselineData() {
		DBTransaction d = new DBTransaction();
		d.runTransaction(Utils.readFileInToList("metadata.sql"));
	}
}
