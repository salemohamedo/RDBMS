package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

     Predicate myPredicate;
     DbIterator myDbIterator;

    public Filter(Predicate p, DbIterator child) {
        myPredicate = p;
        myDbIterator = child;
    }

    public Predicate getPredicate() {
        return myPredicate;
    }

    public TupleDesc getTupleDesc() {
        return myDbIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {

            myDbIterator.open();
            super.open();

    }

    public void close() {
            myDbIterator.close();
            super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
            myDbIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see #filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple tmpTuple = null;
        while(myDbIterator.hasNext()){
            tmpTuple = myDbIterator.next();
            //if it passes the predicate, return the tuple
            if(myPredicate.filter(tmpTuple))
                return tmpTuple;        
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] dbIterator = new DbIterator[1];
        dbIterator[0] = myDbIterator;
        return dbIterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        myDbIterator = children[0];
    }

}
