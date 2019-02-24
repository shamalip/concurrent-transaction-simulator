package sim.application;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import sim.common.AppConfig;

public class ConcurrentSimulatorApp {

	public static void main(String[] args) {
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ThreadPoolExecutor executorPool = new ThreadPoolExecutor(AppConfig.getInt("CORE_POOL_SIZE"),AppConfig.getInt("MAX_POOL_SIZE"),AppConfig.getInt("KEEPALIVE"), TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(AppConfig.getInt("TASKS_IN_QUEUE")), threadFactory);
        for(int i = 0; i < AppConfig.getInt("TASKS_IN_QUEUE"); i++){
            executorPool.execute(new DBTransactionThread("transaction_set"));
        }
        executorPool.shutdown();
	}
}
