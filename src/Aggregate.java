package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */

    DbIterator myChild;
    int myAfield;
    int myGfield;
    Aggregator.Op myAop;
    Aggregator myAgg;
    DbIterator myAggIT;

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	
        myChild = child;
        myAfield = afield;
        myGfield = gfield;
        myAop = aop;

        Type aggtype = myChild.getTupleDesc().getFieldType(myAfield);
        Type gbtype;
        if(gfield > -1){
            gbtype = myChild.getTupleDesc().getFieldType(myGfield);
        }
        else{
            gbtype = null;
        }
        if(aggtype == Type.INT_TYPE){
            myAgg = new IntegerAggregator(myGfield,gbtype,myAfield,myAop);
        }
        else if(aggtype == Type.STRING_TYPE){
            myAgg = new StringAggregator(myGfield,gbtype,myAfield,myAop);
        }


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	   return myGfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	   if(myGfield >= 0){
            return myChild.getTupleDesc().getFieldName(myGfield);
       }
       return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return myAfield;

    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return myChild.getTupleDesc().getFieldName(myAfield);

    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return myAop;

    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        myChild.open();
        while(myChild.hasNext()){
            myAgg.mergeTupleIntoGroup(myChild.next());
        }
        myAggIT = myAgg.iterator();
        myAggIT.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	   if(myAggIT.hasNext()){
            return myAggIT.next();
       }
       return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	   myAggIT.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	   return myChild.getTupleDesc();
    }

    public void close() {
        super.close();
    	myAggIT.close();
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
