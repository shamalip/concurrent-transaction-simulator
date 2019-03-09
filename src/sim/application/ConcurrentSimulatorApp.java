package sim.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import sim.common.AppConfig;
import sim.common.Utils;

public class ConcurrentSimulatorApp {

	private static final String SEP = "|";
	private static final int SIMILATOR_TIMEFRAME = 120000000;
	private static final int TIME_SLOT = 300000;
	private static final int MPL = 50;
	private static final int NUM_THREADS_FOR_INSERTS = 10;
	private static final int NUM_THREADS_FOR_QUERIES = 5;

	public static void main(String[] args) throws InterruptedException, IOException, ParseException {
		//insertBaselineData();	
		startSimulation();
	}

	private static void startSimulation() throws InterruptedException, IOException, ParseException {
		ExecutorService executor = Executors.newFixedThreadPool(MPL);
		BlockingQueue<String> insert_queue = new LinkedBlockingQueue<>();
		BlockingQueue<String> search_queue = new LinkedBlockingQueue<>();
		File insertFile = new File(AppConfig.get("INSERT_FILE"));
		BufferedReader ibr = new BufferedReader(new FileReader(insertFile));
		File queryFile = new File(AppConfig.get("QUERY_FILE"));
		BufferedReader qbr = new BufferedReader(new FileReader(queryFile));
		int time = TIME_SLOT; // Until 5 mins
		long start= System.currentTimeMillis();
		long end = start + SIMILATOR_TIMEFRAME;//run simulator for 20 minutes
		boolean isFirstTime = true;
		String st = null;
		String qt = null;

		while(System.currentTimeMillis() < end) {
			if(!isFirstTime)
			{
				if(Utils.isOnTime(st, time)) {
					insert_queue.add(st.split(SEP)[1]);
					st = null;
				}
			}
			while(st == null && Utils.isOnTime(st = ibr.readLine(), time)) {
				insert_queue.add(st.split(SEP)[1]);
				st = null;
			}

			
			if(!isFirstTime)
			{
				if(Utils.isOnTime(qt, time)) {
					search_queue.add(qt.split(SEP)[1]);
					qt = null;
				}
			}
			while(qt == null && Utils.isOnTime(qt = ibr.readLine(), time)) {
				search_queue.add(qt.split(SEP)[1]);
				qt = null;
			}

			if(isFirstTime) {
				for(int j=0; j < NUM_THREADS_FOR_INSERTS; j++)
				{
					executor.execute(new DBTransactionThread(insert_queue, insert_queue.size() / NUM_THREADS_FOR_INSERTS, "Thread-insert-"+j));
				}
				
				for(int j=0; j < NUM_THREADS_FOR_QUERIES; j++)
				{
					executor.execute(new DBTransactionThread(search_queue, search_queue.size() / NUM_THREADS_FOR_QUERIES, "Thread-query-"+j));
				}
			}
			
			Thread.sleep(3000);
			isFirstTime = false;
			time  = time + TIME_SLOT;
		}
		
		ibr.close();
		qbr.close();
		executor.shutdown();
	}

	private static void insertBaselineData() {
		DBTransaction d = new DBTransaction();
		d.runTransaction(Utils.readFileInToList(AppConfig.get("METADATA_FILE")));
	}
}
