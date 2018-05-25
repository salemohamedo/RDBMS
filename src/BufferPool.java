package simpledb;

import java.io.*;
import java.util.*;

//import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    public static Map<PageId,Page> my_BufferPool;
    public LinkedList<PageId> lruList;
    public static int maxPages;
    public LockManager lockManager;



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */



    public BufferPool(int numPages) {
        my_BufferPool = new HashMap<PageId,Page>();
        maxPages = numPages;
        lruList = new LinkedList<PageId>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }


    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */

    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        
        //aquire a lock or throw transaction aborted exception
        //if deadlock detected
        try{
            lockManager.getLock(tid,(HeapPageId)pid,perm);
        }catch(TransactionAbortedException e){
            throw new TransactionAbortedException();
        }
        
        //if page is already in bufferpool, return it
        if(my_BufferPool.containsKey(pid)){
            return my_BufferPool.get(pid);
        }
        //if bufferpool has reach its limit, eviction needs to occur (not for lab1)
        if (BufferPool.my_BufferPool.size() == maxPages){
            evictPage();
        }
        //find the corresponding page using the pid and add it to bufferpool
        
        int table_id = pid.getTableId();
        Catalog global_cat = Database.getCatalog();
        DbFile db = global_cat.myCatalog.get(table_id).file;
        Page req_page = db.readPage(pid);
        my_BufferPool.put(pid, req_page);
        lruList.addLast(pid);
        return req_page;
        
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.removePageLock(tid,(HeapPageId)pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {

        return lockManager.hasLock(tid,(HeapPageId)p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {

            for(PageId pid : my_BufferPool.keySet()){
                HeapPage hp = (HeapPage)my_BufferPool.get(pid);
                if(hp.dirtyBool && hp.dirtyTid.equals(tid)){              
                    try{
                        if(commit){
                            flushPage(pid);
                        }else{
                            lruList.remove(pid);
                            lruList.add(pid);
                            my_BufferPool.put(pid,hp.getBeforeImage());
                        }
                    }
                    catch(IOException e){
                        throw new IOException("couldn't flush page");
                    }

                }
                else{
                    if(commit){
                        hp.setBeforeImage();
                    }
                }

            }

            lockManager.removeAssociatedLocks(tid);        
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        HeapFile hf = (HeapFile)Database.getCatalog().myCatalog.get(tableId).file;
        ArrayList<Page> pages = hf.insertTuple(tid,t);
        //mark dirtied pages
        //and re add them to back of lrulist
        pages.forEach((page)->{
            HeapPageId id = (HeapPageId)page.getId();
            if(lruList.contains(id)){
                lruList.remove(id);
                lruList.addLast(id);
            } else{
                lruList.addLast(id);
            }
            page.markDirty(true,tid);
            my_BufferPool.put(page.getId(),page);
        });
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile hf = (HeapFile)Database.getCatalog().myCatalog.get(tableId).file;
        ArrayList<Page> pages = hf.deleteTuple(tid,t);
        //mark dirtied pages
        //and re add them to back of lrulist
        pages.forEach((page)->{
            HeapPageId id = (HeapPageId)page.getId();
            if(lruList.contains(id)){
                lruList.remove(id);
                lruList.addLast(id);
            } else{
                lruList.addLast(id);
            }
            page.markDirty(true,tid);
            my_BufferPool.put(page.getId(),page);
        });     
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId id : my_BufferPool.keySet()){
            try{
                flushPage(id);
            }
            catch(IOException io){
                throw new IOException();
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        my_BufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page hp = (HeapPage)my_BufferPool.get(pid);
        if(hp == null){
            throw new IOException("page not in bufferpool");
        }
        if(hp.isDirty() != null){
            HeapFile hf = (HeapFile)Database.getCatalog().myCatalog.get(pid.getTableId()).file;
            hf.writePage(hp);
            hp.markDirty(false, hp.isDirty());
        }
    
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        //withdraws oldest pages from front of LRU linked list and 
        //tries to flush them
        boolean allDirty = true;
        for(Object id : lruList.toArray()){
            
            HeapPageId pid = (HeapPageId)id;
            HeapPage hp = (HeapPage)my_BufferPool.get(pid);
            if(hp.dirtyBool == false){
                allDirty = false;
            try{
                flushPage(pid);
                lruList.remove(pid);
                my_BufferPool.remove(pid);
                break;
            }
            catch(IOException io){
                throw new DbException("couldn't flush page");
            }
        
        }
    }
        if(allDirty){
            throw new DbException("All Pages are Dirty");
        }


}}
