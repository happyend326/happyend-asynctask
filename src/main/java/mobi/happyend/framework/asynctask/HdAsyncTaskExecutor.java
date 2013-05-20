package mobi.happyend.framework.asynctask;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import android.os.Handler;
import android.os.Message;

public class HdAsyncTaskExecutor implements Executor {
	private static final int CORE_POOL_SIZE = 5;
	private static final int MAXIMUM_POOL_SIZE = 256;
	private static final int KEEP_ALIVE = 30;
	private static final int TASK_MAX_TIME = 2 * 60 * 1000;
	private static final int TASK_MAX_TIME_ID = 1;
	private static final int TASK_RUN_NEXT_ID = 2;
	
	private static HdAsyncTaskExecutor sInstance = null;
	private static final ThreadFactory sThreadFactory = new ThreadFactory() {
        private final AtomicInteger mCount = new AtomicInteger(1);
        public Thread newThread(Runnable r) {
        	String log = "HdAsyncTask #" + String.valueOf(mCount.getAndIncrement());
            return new Thread(r, log);
        }
    };

    private static final BlockingQueue<Runnable> sPoolWorkQueue = new SynchronousQueue<Runnable>();
	public static final Executor THREAD_POOL_EXECUTOR
    = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE,
            TimeUnit.SECONDS, sPoolWorkQueue, sThreadFactory, new ThreadPoolExecutor.DiscardPolicy());

	
	private final LinkedList<BdAsyncTaskRunnable> mTasks = new LinkedList<BdAsyncTaskRunnable>();
	private final LinkedList<BdAsyncTaskRunnable> mActives = new LinkedList<BdAsyncTaskRunnable>();
	private final LinkedList<BdAsyncTaskRunnable> mTimeOutActives = new LinkedList<BdAsyncTaskRunnable>();
    
    private Handler mHandler = new Handler(){

		@Override
		public void handleMessage(Message msg) {
			super.handleMessage(msg);
			if(msg.what == TASK_MAX_TIME_ID){
				if(msg.obj != null && msg.obj instanceof BdAsyncTaskRunnable){
					activeTaskTimeOut((BdAsyncTaskRunnable)(msg.obj));
				}
			}else if(msg.what == TASK_RUN_NEXT_ID){
				if(msg.obj != null && msg.obj instanceof BdAsyncTaskRunnable){
					scheduleNext((BdAsyncTaskRunnable)(msg.obj));
				}
			}
		}
		
	};
	
    private HdAsyncTaskExecutor(){}
    
    public String toString(){
    	return "mTasks = " + mTasks.size() + " mActives = " + mActives.size() + " mTimeOutActives = " + mTimeOutActives.size();
    }
    
    public static HdAsyncTaskExecutor getInstance(){
    	if(sInstance == null){
    		sInstance = new HdAsyncTaskExecutor();
    	}
    	return sInstance;
    }
    public synchronized void execute(Runnable r) {
    	if(!(r instanceof HdAsyncTaskFuture)){
    		return;
    	}
    	BdAsyncTaskRunnable runnable = new BdAsyncTaskRunnable((HdAsyncTaskFuture<?>)r) {
            public void run() {
                try {
                	try{
                    	if(getPriority() == HdAsyncTaskPriority.HIGH){
                    		android.os.Process.setThreadPriority(android.os.Process.THREAD_PRIORITY_DEFAULT - 1);
                    	}else if(getPriority() == HdAsyncTaskPriority.MIDDLE){
                    		android.os.Process.setThreadPriority(android.os.Process.THREAD_PRIORITY_DEFAULT);
                    	}else{
                    		android.os.Process.setThreadPriority(android.os.Process.THREAD_PRIORITY_BACKGROUND);
                    	}
                	}catch(Exception ex){
                        ex.printStackTrace();
                	}
                	runTask();
                } finally {
                	if(isSelfExecute() == false){
                		mHandler.sendMessageDelayed(mHandler.obtainMessage(TASK_RUN_NEXT_ID, this), 1);
                	}
                }
            }
        };
        if(runnable.isSelfExecute() == true){
        	new Thread(runnable).start();
        	return;
        }
        
        int num = mTasks.size();
        int index = 0;
        for (index = 0; index < num; index++) {
			if(mTasks.get(index).getPriority() < runnable.getPriority()){
				break;
			}
		}
        mTasks.add(index, runnable);
        scheduleNext(null);
    }
    
    private synchronized void activeTaskTimeOut(BdAsyncTaskRunnable task){
    	mActives.remove(task);
    	mTimeOutActives.add(task);
    	//终止任务需要时间，所以提前终止线程
    	if(mTimeOutActives.size() > MAXIMUM_POOL_SIZE - CORE_POOL_SIZE * 2){
    		BdAsyncTaskRunnable runnable = mTimeOutActives.poll();
    		if(runnable != null){
    			runnable.cancelTask();
    		}
    	}
    	scheduleNext(null);
    }
    
    protected synchronized void scheduleNext(BdAsyncTaskRunnable current) {
    	if(current != null){
    		mActives.remove(current);
    		mTimeOutActives.remove(current);
    		mHandler.removeMessages(TASK_MAX_TIME_ID, current);
        }

    	int activeNum = mActives.size();
        if (activeNum >= CORE_POOL_SIZE) {
            return;
        }
        
        BdAsyncTaskRunnable item = mTasks.peek();
    	if(item == null){
    		return;
    	}

        if (activeNum >= CORE_POOL_SIZE - 1 && item.getPriority() == HdAsyncTaskPriority.LOW){
        	return;
        }
        TypeNum typeNum = new TypeNum(mActives);
        for (int i = 0; i < mTasks.size(); i++) {
        	BdAsyncTaskRunnable task = mTasks.get(i);
			if(typeNum.canExecute(task)){
				mActives.add(task);
	    		mTasks.remove(task);
	            THREAD_POOL_EXECUTOR.execute(task);
	            mHandler.sendMessageDelayed(mHandler.obtainMessage(TASK_MAX_TIME_ID, task), TASK_MAX_TIME);
	            break;
			}
		}
    }
    
    public synchronized void removeAllTask(String tag){
    	removeAllQueueTask(tag);
    	removeTask(mActives, false, tag);
    	removeTask(mTimeOutActives, false, tag);
    }
    
    public synchronized void removeAllQueueTask(String tag){
    	removeTask(mTasks, true, tag);
    }
    
    private void removeTask(LinkedList<BdAsyncTaskRunnable> tasks, boolean remove, String tag){
    	Iterator<BdAsyncTaskRunnable> iterator = tasks.iterator();
        while(iterator.hasNext()) {
        	BdAsyncTaskRunnable next = iterator.next();
        	final String tmp = next.getTag();
        	if(tmp != null && tmp.equals(tag)){
        		if(remove == true){
        			iterator.remove();
        		}
        		next.cancelTask();
			}
        }  
    }
    
    public synchronized void removeTask(HdAsyncTask<?, ?, ?> task){
    	Iterator<BdAsyncTaskRunnable> iterator = mTasks.iterator();
        while(iterator.hasNext()) {
        	BdAsyncTaskRunnable next = iterator.next();
        	if(next != null && next.getTask() == task){
        		iterator.remove();
        		break;
			}
        }  
    }
    
    public synchronized HdAsyncTask<?, ?, ?> searchTask(String key){
    	HdAsyncTask<?, ?, ?> tmp = null;
    	tmp = searchTask(mTasks, key);
    	if(tmp == null){
    		tmp = searchTask(mActives, key);
    	}
    	return tmp;
    }
    
    public HdAsyncTask<?, ?, ?> searchTask(LinkedList<BdAsyncTaskRunnable> list, String key){
    	if(list == null){
    		return null;
    	}
    	Iterator<BdAsyncTaskRunnable> iterator = list.iterator();
        while(iterator.hasNext()) {
        	BdAsyncTaskRunnable next = iterator.next();
        	final String tmp = next.getKey();
        	if(tmp != null && tmp.equals(key)){
        		return next.getTask();
			}
        }
        return null;
    }
    
    
    private static abstract class BdAsyncTaskRunnable implements Runnable {
    	private HdAsyncTaskFuture<?> mHdAsyncTaskFuture = null;
    	
    	public BdAsyncTaskRunnable(HdAsyncTaskFuture<?> task){
    		if (task == null){
                throw new NullPointerException();
    		}
    		mHdAsyncTaskFuture = task;
    	}
    	
    	public void runTask(){
    		if(mHdAsyncTaskFuture != null){
    			mHdAsyncTaskFuture.run();
    		}
    	}
    	
    	public void cancelTask(){
    		if(mHdAsyncTaskFuture != null){
    			mHdAsyncTaskFuture.cancelTask();
    		}
    	}

    	public HdAsyncTask<?, ?, ?> getTask(){
    		if(mHdAsyncTaskFuture != null){
    			return mHdAsyncTaskFuture.getTask();
    		}else{
    			return null;
    		}
    	}
    	
    	public int getPriority(){
    		try {
    			return mHdAsyncTaskFuture.getTask().getPriority();
			} catch (Exception e) {
				return HdAsyncTaskPriority.LOW;
			}
    	}

    	public String getTag(){
    		try {
    			return mHdAsyncTaskFuture.getTask().getTag();
			} catch (Exception e) {
				return null;
			}
    	}
    	
    	public String getKey(){
    		try {
    			return mHdAsyncTaskFuture.getTask().getKey();
			} catch (Exception e) {
				return null;
			}
    	}
    	
    	public HdAsyncTaskType getType(){
    		try {
    			return mHdAsyncTaskFuture.getTask().getType();
			} catch (Exception e) {
				return HdAsyncTaskType.MAX_PARALLEL;
			}
    	}
    	
    	public boolean isSelfExecute() {
    		try {
    			return mHdAsyncTaskFuture.getTask().isSelfExecute();
    		} catch (Exception e) {
				return false;
			}
    	}
	}
    
    private class TypeNum{
    	int mTotalNum = 0;
    	int mTypeSerialNum = 0;
    	int mTypeTwoNum = 0;
    	int mTypeThreeNum = 0;
    	int mTypeFourNum = 0;

    	public TypeNum(LinkedList<BdAsyncTaskRunnable> list){
    		if(list == null){
    			return ;
    		}
    		mTotalNum = list.size();
    		for (int i = 0; i < mTotalNum; i++) {
    			BdAsyncTaskRunnable tmp = list.get(i);
				if(tmp.getType() == HdAsyncTaskType.SERIAL){
					mTypeSerialNum++;
				}else if(tmp.getType() == HdAsyncTaskType.TWO_PARALLEL){
					mTypeTwoNum++;
				}else if(tmp.getType() == HdAsyncTaskType.THREE_PARALLEL){
					mTypeThreeNum++;
				}else if(tmp.getType() == HdAsyncTaskType.FOUR_PARALLEL){
					mTypeFourNum++;
				}
			}
    	}
    	
    	public boolean canExecute(BdAsyncTaskRunnable task){
    		if(task == null){
    			return false;
    		}
    		if(task.getType() == HdAsyncTaskType.SERIAL){
				if(mTypeSerialNum < 1){
					return true;
				}
			}else if(task.getType() == HdAsyncTaskType.TWO_PARALLEL){
				if(mTypeTwoNum < 2){
					return true;
				}
			}else if(task.getType() == HdAsyncTaskType.THREE_PARALLEL){
				if(mTypeThreeNum < 3){
					return true;
				}
			}else if(task.getType() == HdAsyncTaskType.FOUR_PARALLEL){
				if(mTypeFourNum < 4){
					return true;
				}
			}else{
				return true;
			}
    		return false;
    	}
    }

}
