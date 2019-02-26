package sim.application;

import java.util.List;

public class DBTransactionThread extends DBTransaction implements Runnable  {

	List<String> listOfStatements;
	
	public DBTransactionThread(List<String> trSet) {
		listOfStatements = trSet;	
	}

	@Override
	public void run() {
		runTransaction(listOfStatements);
	}	
}
