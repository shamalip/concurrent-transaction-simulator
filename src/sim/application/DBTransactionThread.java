package sim.application;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class DBTransactionThread extends DBTransaction implements Runnable  {

	BlockingQueue<String> queue; 
	int numQueriesPerThread = 0;

	public DBTransactionThread(BlockingQueue<String> queue, int numQueriesPerThread, String threadName) {
		this.queue = queue;	
		this.numQueriesPerThread = numQueriesPerThread;
		super.threadName = threadName;
	}

	@Override
	public void run() {
		long start= System.currentTimeMillis();
		long end = start+120000000;//run simulator for 20 minutes
		while(System.currentTimeMillis() < end)
		{
			while(queue.size() > 0) 
			{
				List<String> queries = new ArrayList<>();
				for(int i = 0; i < numQueriesPerThread && i < queue.size(); i++) 
				{
					String query = queue.poll();
					if(null != query) 
					{
						queries.add(query);	
					}
				}		
				runTransaction(queries);
			}
			try 
			{
				Thread.sleep(300);
			} 
			catch (InterruptedException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}	
}
