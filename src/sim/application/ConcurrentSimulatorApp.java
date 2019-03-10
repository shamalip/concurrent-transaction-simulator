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

	private static final String SEP = "\\|";
	private static final long SIMILATOR_TIMEFRAME = 900000;//run for 15 minutes
	private static final long TIME_SLOT = 300000L;
	private static final int MPL = 10;
	private static final int NUM_THREADS_FOR_INSERTS = 7;
	private static final int NUM_THREADS_FOR_QUERIES = 3;
	private static final int TRANSACTION_MAX_INSERT_SIZE = 200;
	private static final int TRANSACTION_MAX_QUERY_SIZE = 2;

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
		long time = 1510128000000L + TIME_SLOT ; // Until 5 mins
		long start= System.currentTimeMillis();
		long end = start + SIMILATOR_TIMEFRAME;//run simulator for 20 minutes
		boolean isFirstTime = true;
		String st = null;
		String qt = null;


		while(System.currentTimeMillis() < end) {
			System.out.println("TIME "+time);
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
			while(qt == null && Utils.isOnTime(qt = qbr.readLine(), time)) {
				search_queue.add(qt.split(SEP)[1]);
				qt = null;
			}

			if(isFirstTime) {
				for(int j=0; j < NUM_THREADS_FOR_INSERTS; j++)
				{
					executor.execute(new DBTransactionThread(insert_queue, TRANSACTION_MAX_INSERT_SIZE , "Thread-insert-"+j));
				}

				for(int j=0; j < NUM_THREADS_FOR_QUERIES; j++)
				{
					executor.execute(new DBTransactionThread(search_queue, TRANSACTION_MAX_QUERY_SIZE , "Thread-query-"+j));
				}

			}

			Thread.sleep(10000);
			isFirstTime = false;
			time  = time + TIME_SLOT;
		}

		ibr.close();
		qbr.close();
		System.out.println("Throughput " + DBTransaction.getTotalNumOfTransactions());
		System.out.println("Average Query Time "+DBTransaction.getTotalQueryTime() / DBTransaction.getTotalNumOfQueries());
		System.out.println("Average overall time "+DBTransaction.getTotalTransactionTime() / DBTransaction.getTotalNumOfTransactions());
		System.out.println("----------------------------------\n");
		System.out.println("Total Transactions time " + DBTransaction.getTotalTransactionTime());
		System.out.println("Total Queries time " + DBTransaction.getTotalQueryTime());
		System.out.println("Total Number of queries time " + DBTransaction.getTotalNumOfQueries());
		System.out.println("Total operations in a transaction(TRANSACTION SIZE) " + DBTransaction.getTotalTranSize());
		System.out.println("Average transaction size " + DBTransaction.getTotalTranSize() / DBTransaction.getTotalNumOfTransactions());
		executor.shutdown();
		System.exit(0);
	}

	private static void insertBaselineData() throws IOException {
		DBTransaction d = new DBTransaction();
		d.runTransaction(Utils.readFileInToList(AppConfig.get("METADATA_FILE")));
	}
}
