package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_aggregateIndex if there is no aggregateIndex
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no aggregateIndex
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    int myGbField;
    Type myGbFieldType;
    int myAfield;
    Op myWhat;
    boolean noGroup;
    HashMap<Field,AggregateData> aggMap;
    int aggregateIndex;

    public class AggregateData{
        int myCount;
        public AggregateData(int count){
            myCount = count;
        }
        public void incCount(){
            myCount++;
        }
        public int getCount(){
            return myCount;
        }

    }

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(what != Op.COUNT){
            throw new IllegalArgumentException("operation must be count");
        }
        myGbField = gbfield;
        myGbFieldType = gbfieldtype;
        myAfield = afield;
        myWhat = what;
        if(gbfield == -1){
            noGroup = true;
            aggregateIndex = 0;
        }
        else{
            noGroup = false;
            aggregateIndex = 1;
        }
        //holds the aggregate values for a given field
        //if no groupby, field is default to null
        aggMap = new HashMap<Field,AggregateData>();
    }

    /**
     * Merge a new tuple into the aggregate, aggregateIndex as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public Tuple newAggregateTuple(){


        Type[] tdTypes;
        String[] tdNames; 


        if(noGroup){
            tdNames = new String[1];
            tdNames[0] = new String("Aggregate");
            tdTypes = new Type[1];
            tdTypes[0] = Type.INT_TYPE;
        }
        else{
            tdNames = new String[2];
            tdTypes = new Type[2];
            tdNames[0] = new String("Group By");
            tdNames[1] = new String("Aggregate");
            tdTypes[0] = myGbFieldType;
            tdTypes[1] = Type.INT_TYPE;
        }
        return new Tuple(new TupleDesc(tdTypes,tdNames));

    }

public void mergeTupleIntoGroup(Tuple tup) {

        Field myField;
        if(noGroup)
            myField = null;
        else
            myField = tup.getField(myGbField);

        AggregateData myFieldData = aggMap.get(myField);

        //if new field, add to aggMap
        if(myFieldData == null){
            myFieldData = new AggregateData(1);
            aggMap.put(myField,myFieldData);
        }else{
            myFieldData.incCount();
            aggMap.put(myField,myFieldData);
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   aggregateIndex. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        
        Vector<Tuple> aggTuples = new Vector<Tuple>();
        Tuple aggTuple;

        for(Field field : aggMap.keySet()){

            aggTuple = newAggregateTuple();
            AggregateData aggData = aggMap.get(field);
            int aggValue = aggData.getCount();
            if(!noGroup){
                aggTuple.setField(0,field);
            }
            aggTuple.setField(aggregateIndex,new IntField(aggValue));
            aggTuples.add(aggTuple);
        }

        return new TupleIterator(newAggregateTuple().getTupleDesc(),aggTuples);

    }

}
