package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    TransactionId myTID;
    DbIterator myChild;
    TupleDesc myTD;
    boolean alreadyCalled;

    public Delete(TransactionId t, DbIterator child) {
        myTID = t;
        myChild = child;
        Type[] int_type = new Type[1];
        int_type[0] =  Type.INT_TYPE;
        myTD = new TupleDesc(int_type);
        //ensures fetch next only every called once
        alreadyCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return myTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        myChild.open();
        alreadyCalled = false;
    }

    public void close() {
        super.close();
        myChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        open();
        close();
        myChild.rewind();

    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(alreadyCalled){
            return null;
        }
        alreadyCalled = true;
        BufferPool bp = Database.getBufferPool();
        int numDeletes = 0;
        while(myChild.hasNext()){
            try{
            bp.deleteTuple(myTID,myChild.next());
            numDeletes++;
        }catch(IOException io){
            throw new DbException("IOException from tuple insert");
        }
        }
        Tuple numDeletesTuple = new Tuple(myTD);
        numDeletesTuple.setField(0,new IntField(numDeletes));
        return numDeletesTuple;
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
