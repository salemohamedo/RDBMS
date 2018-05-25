package simpledb;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {

	TransactionId myTID;
	Iterator<Tuple> tuple_it;
	Permissions read = Permissions.READ_ONLY;
	int page_num = 0;
	HeapFile myHeapFile;


	public HeapFileIterator(TransactionId tid, HeapFile hf) {
		this.myTID = tid;
		this.myHeapFile = hf;
	}

	public void open() throws DbException, TransactionAbortedException{
		page_num = 0;
		//access first heappage
		PageId pid = new HeapPageId(myHeapFile.getId(),page_num);
		HeapPage hp = (HeapPage)Database.getBufferPool().getPage(myTID,pid,read);
		tuple_it = hp.iterator();
	}

	public boolean hasNext() throws DbException, TransactionAbortedException {
		if(tuple_it == null)
			return false;
		if(tuple_it.hasNext())
			return true;
		else{
			if(page_num >= myHeapFile.numPages() - 1)
				return false;
			else{
				//access next heappage if fully gone through current one
				PageId pid = new HeapPageId(myHeapFile.getId(), page_num + 1);
				HeapPage hp = (HeapPage) Database.getBufferPool().getPage(myTID,pid,read);
				return hp.iterator().hasNext();
			}
		}
	}

	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if(tuple_it == null)
			throw new NoSuchElementException("none");
		if(tuple_it.hasNext())
			return tuple_it.next();
		else{
			PageId pid = new HeapPageId(myHeapFile.getId(), page_num + 1);
			HeapPage hp = (HeapPage) Database.getBufferPool().getPage(myTID,pid,read);
			if(hp.iterator().hasNext()){
				page_num++;
				tuple_it = hp.iterator();
				return tuple_it.next();
			}
			throw new NoSuchElementException();
		}
	}

	public void rewind() throws DbException, TransactionAbortedException{
		close();
		open();
	}

	public void close(){
		page_num = 0;
		tuple_it = null;
	}
}