package mobi.happyend.framework.asynctask;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public abstract class HdAsyncTaskFuture<V> extends FutureTask<V> {
	private HdAsyncTask<?, ?, ?> mTask = null;
		
	public HdAsyncTask<?, ?, ?> getTask(){
		return mTask;
	}
	public HdAsyncTaskFuture(Callable<V> callable, HdAsyncTask<?, ?, ?> task) {
		super(callable);
		mTask = task;
	}
	
	public HdAsyncTaskFuture(Runnable runnable, V result, HdAsyncTask<?, ?, ?> task) {
		super(runnable, result);
		mTask = task;
	}
	
	protected abstract void cancelTask();

}
