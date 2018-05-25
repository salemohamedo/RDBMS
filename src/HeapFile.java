package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public File myFile;

    public TupleDesc myTD;
    int addedPages = 0;
    public HeapFile(File f, TupleDesc td) {
        myFile = f;
        myTD = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return myFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return myFile.getAbsoluteFile().hashCode();

    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return myTD;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        try{
            RandomAccessFile raf = new RandomAccessFile(myFile,"r");
            //calculate and seek to offset
            int offset = BufferPool.getPageSize()*pid.getPageNumber();
            raf.seek(offset);
            //read into buffer
            byte[] buffer = new byte[BufferPool.getPageSize()];
            raf.read(buffer,0,BufferPool.getPageSize());
            raf.close();
            return new HeapPage((HeapPageId) pid,buffer);
        }
        catch(IOException ex){
            //throw new IOException("failed");
            System.out.print(ex);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf;
        int offset = page.getId().getPageNumber();
        try{
            raf = new RandomAccessFile(myFile,"rw");
            raf.seek(offset*BufferPool.getPageSize());
            raf.write(page.getPageData());;
            raf.close();
        }catch(IOException io){
            throw io;
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor((myFile.length()/BufferPool.getPageSize())) + addedPages;
    }
    

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        if(!getTupleDesc().equals(t.getTupleDesc())){
            throw new DbException("Tuple to be inserted has mismatched Tuple Desc");
        }
        ArrayList<Page> pages = new ArrayList<Page>();
        HeapPageId pid;
        HeapPage hp;
        int i = 0;
    
        while(i < numPages()){
            pid = new HeapPageId(getId(), i);
            hp = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);

            //no more empty slots, get the next page in heap file
            if(hp.getNumEmptySlots() == 0){
                i++;
                pid = new HeapPageId(getId(),i);
                //have searched every page in heapfile, create a new empty one and
                //write it
                if(i >= numPages()){
                    hp = new HeapPage(pid, HeapPage.createEmptyPageData());
                    RandomAccessFile raf = new RandomAccessFile(myFile, "rw");
                    int offset = BufferPool.getPageSize() * numPages();
                    raf.seek(offset);
                    raf.write(hp.getPageData(), 0, BufferPool.getPageSize());
                    raf.close();
                }
            }
            else{
                //found a free slot, insert it to the page
                pages.add(hp);
                hp.insertTuple(t);
                break;
            }
            
        }


        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        ArrayList<Page> pages = new ArrayList<Page>();
        HeapPageId pid = new HeapPageId(getId(),t.getRecordId().getPageId().getPageNumber());
        HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        pg.deleteTuple(t);
        pages.add(pg);
        return pages;

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

