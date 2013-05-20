package mobi.happyend.framework.asynctask;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import android.os.Handler;
import android.os.Message;


public abstract class HdAsyncTask<Params, Progress, Result> {
	private static final int MESSAGE_POST_RESULT = 0x1;
    private static final int MESSAGE_POST_PROGRESS = 0x2;
    
    private static final HdAsyncTaskExecutor sDefaultExecutor = HdAsyncTaskExecutor.getInstance();
    private static InternalHandler sHandler = new InternalHandler();
    
    private final WorkerRunnable<Params, Result> mWorker;
    private final HdAsyncTaskFuture<Result> mFuture;
    private volatile BdAsyncTaskStatus mStatus = BdAsyncTaskStatus.PENDING;
    private int mPriority = HdAsyncTaskPriority.LOW;
	private String mTag = null;
	private String mKey = null;
	private HdAsyncTaskType mType = HdAsyncTaskType.MAX_PARALLEL;
	private boolean isSelfExecute = false;
    
    private final AtomicBoolean mTaskInvoked = new AtomicBoolean();

    public enum BdAsyncTaskStatus {
        PENDING, RUNNING, FINISHED,
    }

    public HdAsyncTask() {
        mWorker = new WorkerRunnable<Params, Result>() {
            public Result call() throws Exception {
                mTaskInvoked.set(true);
                return postResult(doInBackground(mParams));
            }
        };

        mFuture = new HdAsyncTaskFuture<Result>(mWorker, this) {
            @Override
            protected void done() {
                try {
                    final Result result = get();
                    postResultIfNotInvoked(result);
                } catch (InterruptedException e) {
                    
                } catch (ExecutionException e) {
                    throw new RuntimeException("An error occured while executing doInBackground()",
                            e.getCause());
                } catch (CancellationException e) {
                    postResultIfNotInvoked(null);
                } catch (Throwable t) {
                    throw new RuntimeException("An error occured while executing "
                            + "doInBackground()", t);
                }
            }

			@Override
			protected void cancelTask() {
				// TODO Auto-generated method stub
				HdAsyncTask.this.cancel(true);
			}
        };
    }
    
    public static void removeAllTask(String tag){
    	sDefaultExecutor.removeAllTask(tag);
    }
    
    public static void removeAllQueueTask(String tag){
    	sDefaultExecutor.removeAllQueueTask(tag);
    }
    
    public static HdAsyncTask<?, ?, ?> searchTask(String key){
    	return sDefaultExecutor.searchTask(key);
    }
    public int setPriority(int priority){
    	if(mStatus != BdAsyncTaskStatus.PENDING){
			throw new IllegalStateException("the task is already running");
		}
		int old = mPriority;
		mPriority = priority;
		return old;
	}
	
	public int getPriority(){
		return mPriority;
	}
	
	public String getTag() {
		return mTag;
	}

	public String setTag(String tag) {
		if(mStatus != BdAsyncTaskStatus.PENDING){
			throw new IllegalStateException("the task is already running");
		}
		String tmp = mTag;
		mTag = tag;
		return tmp;
	}

	public String getKey() {
		return mKey;
	}

	public String setKey(String key) {
		if(mStatus != BdAsyncTaskStatus.PENDING){
			throw new IllegalStateException("the task is already running");
		}
		String tmp = mKey;
		mKey = key;
		return tmp;
	}
	
	public HdAsyncTaskType getType() {
		return mType;
	}

	public void setType(HdAsyncTaskType type) {
		if(mStatus != BdAsyncTaskStatus.PENDING){
			throw new IllegalStateException("the task is already running");
		}
		mType = type;
	}
	
    private void postResultIfNotInvoked(Result result) {
        final boolean wasTaskInvoked = mTaskInvoked.get();
        if (!wasTaskInvoked) {
            postResult(result);
        }
    }

    @SuppressWarnings("unchecked")
	private Result postResult(Result result) {
        Message message = sHandler.obtainMessage(MESSAGE_POST_RESULT,
                new BdAsyncTaskResult<Result>(this, result));
        message.sendToTarget();
        return result;
    }

    public final BdAsyncTaskStatus getStatus() {
        return mStatus;
    }
 
    protected abstract Result doInBackground(Params... params);

    public void cancel(){
    	cancel(true);
    }
    protected void onPreCancel(){

    }
    
    protected void onPreExecute() {
    }

    protected void onPostExecute(Result result) {
    }


    protected void onProgressUpdate(Progress... values) {
    }

    protected void onCancelled(Result result) {
        onCancelled();
    }    
    
    protected void onCancelled() {
    }

    public final boolean isCancelled() {
        return mFuture.isCancelled();
    }

    public final boolean cancel(boolean mayInterruptIfRunning) {
    	onPreCancel();
    	sDefaultExecutor.removeTask(this);
        return mFuture.cancel(mayInterruptIfRunning);
    }

    public final Result get() throws InterruptedException, ExecutionException {
        return mFuture.get();
    }


    public final Result get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        return mFuture.get(timeout, unit);
    }

    public final HdAsyncTask<Params, Progress, Result> execute(Params... params) {
        return executeOnExecutor(sDefaultExecutor, params);
    }


    public final HdAsyncTask<Params, Progress, Result> executeOnExecutor(Executor exec,
            Params... params) {
        if (mStatus != BdAsyncTaskStatus.PENDING) {
            switch (mStatus) {
                case RUNNING:
                    throw new IllegalStateException("Cannot execute task:"
                            + " the task is already running.");
                case FINISHED:
                    throw new IllegalStateException("Cannot execute task:"
                            + " the task has already been executed "
                            + "(a task can be executed only once)");
            }
        }

        mStatus = BdAsyncTaskStatus.RUNNING;

        onPreExecute();

        mWorker.mParams = params;
        exec.execute(mFuture);

        return this;
    }

 
    protected final void publishProgress(Progress... values) {
        if (!isCancelled()) {
            sHandler.obtainMessage(MESSAGE_POST_PROGRESS,
                    new BdAsyncTaskResult<Progress>(this, values)).sendToTarget();
        }
    }

    private void finish(Result result) {
        if (isCancelled()) {
            onCancelled(result);
        } else {
            onPostExecute(result);
        }
        mStatus = BdAsyncTaskStatus.FINISHED;
    }

    public boolean isSelfExecute() {
		return isSelfExecute;
	}

	public void setSelfExecute(boolean isSelfExecute) {
		if(mStatus != BdAsyncTaskStatus.PENDING){
			throw new IllegalStateException("the task is already running");
		}
		this.isSelfExecute = isSelfExecute;
	}

	private static class InternalHandler extends Handler {
        @SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
        public void handleMessage(Message msg) {
            BdAsyncTaskResult result = (BdAsyncTaskResult) msg.obj;
            switch (msg.what) {
                case MESSAGE_POST_RESULT:
                    // There is only one result
                    result.mTask.finish(result.mData[0]);
                    break;
                case MESSAGE_POST_PROGRESS:
                    result.mTask.onProgressUpdate(result.mData);
                    break;
            }
        }
    }

    private static abstract class WorkerRunnable<Params, Result> implements Callable<Result> {
    	Params[] mParams;
    }

    private static class BdAsyncTaskResult<Data> {
        @SuppressWarnings("rawtypes")
		final HdAsyncTask mTask;
        final Data[] mData;

        @SuppressWarnings("rawtypes")
		BdAsyncTaskResult(HdAsyncTask task, Data... data) {
            mTask = task;
            mData = data;
        }
    }
    
}
