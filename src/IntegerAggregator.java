package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_aggregateIndex if there is no aggregateIndex
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no aggregateIndex
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    int myGbField;
    Type myGbFieldType;
    int myAfield;
    Op myWhat;
    boolean noGroup;
    int aggregateIndex;
    HashMap<Field,AggregateData> aggMap;

    public class AggregateData{
        int myValue;
        int myCount;
        public AggregateData(int value,int count){
            myValue = value;
            myCount = count;
        }
        public void setValue(int value){
            myValue = value;
        }
        public void incCount(){
            myCount++;
        }
        public int getCount(){
            return myCount;
        }
        public int getValue(){
            return myValue;
        }
        public void addValue(int value){
            myValue += value;
        }
    }


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, aggregateIndex as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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

        int tupValue = ((IntField)tup.getField(myAfield)).getValue();

        if(myFieldData == null){
            myFieldData = new AggregateData(tupValue,1);
            aggMap.put(myField,myFieldData);
        }else{

            if(myWhat == Op.MIN){
                if(tupValue < myFieldData.getValue()){
                    myFieldData.setValue(tupValue);
                }
            }

            else if(myWhat == Op.MAX){
                if(tupValue > myFieldData.getValue()){
                    myFieldData.setValue(tupValue);
                }
            }

            else if(myWhat == Op.SUM || myWhat == Op.AVG || myWhat == Op.COUNT){
                myFieldData.addValue(tupValue);

            }

            myFieldData.incCount();
            aggMap.put(myField,myFieldData);
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no aggregateIndex. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        
        Vector<Tuple> aggTuples = new Vector<Tuple>();
        Tuple aggTuple;

        for(Field field : aggMap.keySet()){

            aggTuple = newAggregateTuple();
            AggregateData aggData = aggMap.get(field);
            int aggValue = 0;

            if(myWhat == Op.SUM || myWhat == Op.MAX || myWhat == Op.MIN){
                aggValue = aggData.getValue();
            }
            else if(myWhat == Op.COUNT){
                aggValue = aggData.getCount();
            }
            else if(myWhat == Op.AVG){
                if(aggData.getCount() == 0){
                    aggValue = 0;
                }
                else{
                    aggValue = aggData.getValue()/aggData.getCount();
                }
            }
            if(!noGroup){
                aggTuple.setField(0,field);
            }
            aggTuple.setField(aggregateIndex,new IntField(aggValue));
            aggTuples.add(aggTuple);
        }

        return new TupleIterator(newAggregateTuple().getTupleDesc(),aggTuples);

    }

}
