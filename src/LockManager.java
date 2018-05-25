package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager{

	public class Lock{

		Vector<TransactionId> sharedLocks;
		boolean sharedLock;
		TransactionId currentExclusiveLock;

		public Lock(Boolean shared){
			sharedLock = shared;
			currentExclusiveLock = null;
			sharedLocks = new Vector<TransactionId>();
		}

		public void addSharedLock(TransactionId tid){
			if(!sharedLocks.contains(tid)){
				sharedLocks.add(tid);
			}
		}
	}

	public ConcurrentHashMap<HeapPageId,Lock> lockMap;

	public LockManager(){
		lockMap = new ConcurrentHashMap<HeapPageId,Lock>();
	}

	public synchronized void block(long start, long interval) throws TransactionAbortedException{


		if(System.currentTimeMillis() - start > interval){
			throw new TransactionAbortedException();
		}
		try{
			wait(interval);
		}catch(Exception e){
			System.out.println(e);
		}

	}

	public synchronized void getLock(TransactionId tid, HeapPageId pid, Permissions perm) throws TransactionAbortedException{

			if(perm.permLevel == 0){
				 getSharedLock(tid,pid);
			}
			else if(perm.permLevel == 1){
				 getExclusiveLock(tid,pid);
			}
			else{
				return;
			}
		}
	


	private  void getSharedLock(TransactionId tid, HeapPageId pid) throws TransactionAbortedException{
		long start = System.currentTimeMillis();
		long interval = 200;
		while(true){
			//no exclusive or shared locks exist
			if(!lockMap.containsKey(pid)){
				Lock lock = new Lock(true);
				lock.addSharedLock(tid);
				lockMap.put(pid,lock);
				return;
			}
			else{
				Lock lock = lockMap.get(pid);
				//an exclusive lock exists
				if(lock.sharedLock == false){
					//current has an exclusive lock
					//could technically downgrade but 
					//unnecessary
					if(hasExclusiveLock(tid,pid)){
						return;
					}
					else{
						//another transaction has an exclusive
						//lock - block until available
						block(start, interval);
					}
				}
				else{
					lock.addSharedLock(tid);
					lockMap.put(pid,lock);
					return;
				}
			}
		}
}

	private  void getExclusiveLock(TransactionId tid, HeapPageId pid) throws TransactionAbortedException{
		
		long start = System.currentTimeMillis();
		long interval = 200;
		while(true){
		//no shared or exclusive lock exists
			if(!lockMap.containsKey(pid)){
				Lock lock = new Lock(false);
				lock.currentExclusiveLock = tid;
				lockMap.put(pid,lock);
				return;
			}
			else{
				Lock lock = lockMap.get(pid);
				//currently is an exclusive lock
				if(lock.sharedLock == false){
					if(hasExclusiveLock(tid,pid)){
						return;
					}
					else{
						//wait till other transaction 
						//unlocks
						block(start, interval);
					}
				}
				//current is a shared lock
				else{
					//is this transaction already holds a shared lock on the page
					//and it is the only transaction that has one - upgrade it
					//to a exclusive lock
					if(hasSharedLock(tid,pid) && (lock.sharedLocks.size() == 1)){
						upgradeLock(tid,pid);
						return;
					}
					//does not have a shared lock on the page or more than one
					//transaction has a shared lock - block till one becomes available
					else{
						block(start, interval);
					}
				}
			}			

		}
	}


	public  boolean hasLock(TransactionId tid, HeapPageId pid){

		if(hasSharedLock(tid,pid) || hasExclusiveLock(tid,pid)){
			return true;
		}
		return false;

	}

	public  boolean hasSharedLock(TransactionId tid, HeapPageId pid){

		Lock lock = lockMap.get(pid);
		if(lock == null){
			return false;
		}
		else{
			if(lock.sharedLock == false){
				return false;
			}
			else{
				if(lock.sharedLocks.contains(tid)){
					return true;
				}
				else{
					return false;
				}
			}
		}

	}

	public  boolean hasExclusiveLock(TransactionId tid, HeapPageId pid){

		Lock lock = lockMap.get(pid);
		if(lock == null){
			return false;
		}
		else{
			if(lock.sharedLock == true){
				return false;
			}
			else{
				if(lock.currentExclusiveLock.equals(tid)){
					return true;
				}
				else{
					return false;
				}
			}
		}

	}


	/* Method to upgrade a shared lock to an exclusive lock
	only works if the current Lock only has one transaction
	participating in the shared lock, and that is the one requesting
	an exclusive lock */

	private synchronized void upgradeLock(TransactionId tid, HeapPageId pid){
		Lock lock = lockMap.get(pid);
		lock.sharedLocks.clear();
		lock.currentExclusiveLock = tid;
		lock.sharedLock = false;
		lockMap.put(pid,lock);
		return;

	}

	public  synchronized void removePageLock(TransactionId tid, HeapPageId pid){

		if(hasSharedLock(tid,pid)){
			Lock lock = lockMap.get(pid);
			lock.sharedLocks.remove(tid);
			if(lock.sharedLocks.size() == 0){
				lockMap.remove(pid);
			}
			//no locks remaining, stop tracking
			else{
				lockMap.put(pid,lock);
			}
		}
		else if(hasExclusiveLock(tid,pid)){
			lockMap.remove(pid);
		}
	}

	public  synchronized void removeAssociatedLocks(TransactionId tid){
		for(HeapPageId pid : lockMap.keySet()){
			removePageLock(tid,pid);
		}
	}

}

