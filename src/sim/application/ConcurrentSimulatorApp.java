package sim.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import sim.common.AppConfig;
import sim.common.Utils;

public class ConcurrentSimulatorApp {

	private static final int MPL = 50;
	private static final int NUM_QUERIES_PER_THREAD = 100;

	public static void main(String[] args) throws InterruptedException, IOException {
		insertBaselineData();	
		startSimulation();
	}

	private static void startSimulation() throws InterruptedException, IOException {
		ExecutorService executor = Executors.newFixedThreadPool(MPL);
		BlockingQueue<String> insert_queue = new LinkedBlockingQueue<>();
		BlockingQueue<String> search_queue = new LinkedBlockingQueue<>();

		// 20 files will be used.
		for(int i = 1; i <= AppConfig.getInt("TOTAL_FILES_COUNT"); i++){ 
			File file = new File(AppConfig.get("INSERT_FILE")+7+i+".sql"); 
			BufferedReader br = new BufferedReader(new FileReader(file)); 
			String st; 
			while ((st = br.readLine()) != null) {
				insert_queue.add(st);
			} 
			br.close();
			file = new File(AppConfig.get("SEARCH_FILE")+7+i+".sql"); 
			br = new BufferedReader(new FileReader(file)); 
			while ((st = br.readLine()) != null) {
				insert_queue.add(st);
			} 
			br.close();	
			if(i == 1) {
				for(int j=0; j < 40; j++)
					executor.execute(new DBTransactionThread(insert_queue, NUM_QUERIES_PER_THREAD, "Thread-"+j));

				for(int j=0; j < 10; j++)
					executor.execute(new DBTransactionThread(search_queue, NUM_QUERIES_PER_THREAD, "Thread-"+j));
			}
			
			Thread.sleep(300000);
		}
		executor.shutdown();
	}

	private static void insertBaselineData() {
		DBTransaction d = new DBTransaction();
		d.runTransaction(Utils.readFileInToList(AppConfig.get("METADATA_FILE")));
	}
}
