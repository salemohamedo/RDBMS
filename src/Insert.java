package simpledb;
import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    TransactionId myTID;
    DbIterator myChild;
    int myTableId;
    TupleDesc myTD;
    boolean alreadyCalled;


    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        myTID = t;
        myChild = child;
        myTableId = tableid;
        Type[] int_type = new Type[1];
        int_type[0] =  Type.INT_TYPE;
        myTD = new TupleDesc(int_type);
        //ensures the fetchnext is only called successfully once
        alreadyCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return myTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        myChild.open();
        super.open();

    }

    public void close() {
        super.close();
        myChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        myChild.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(alreadyCalled){
            return null;
        }
        alreadyCalled = true;
        int numInserts = 0;
        while(myChild.hasNext()){
            try{
            Database.getBufferPool().insertTuple(myTID,myTableId,myChild.next());
        }catch(IOException io){
            throw new DbException("IOException from tuple insert");
        }
            numInserts++;
        }
        Tuple insertsTuple = new Tuple(myTD);
        insertsTuple.setField(0,new IntField(numInserts));
        return insertsTuple;
    }

    @Override
    public DbIterator[] getChildren() {

        DbIterator[] dbIterator = new DbIterator[1];
        dbIterator[0] = myChild;
        return dbIterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        myChild = children[0];
    }
}
