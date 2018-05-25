package simpledb;

import java.util.concurrent.ConcurrentHashMap;

import java.util.*;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    int myIOCOST;
    int numTuples;
    HeapFile myHeapFile;
    TupleDesc myTD;
    Map<Integer,FieldHistogram> tableHistograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */

    public class FieldHistogram {

        int myMax;
        int myMin;
        IntHistogram myIntHistogram;
        StringHistogram myStrHistogram;

        public FieldHistogram(int min, int max){
            myMin = min;
            myMax = max;
        }

        public void setMin(int min){
            myMin = min;
        }

        public void setMax(int max){
            myMax = max;
        }

        public void initINTHist(){
            myIntHistogram = new IntHistogram(NUM_HIST_BINS,myMin,myMax);

        }
        public void initSTRHist(){
            myStrHistogram = new StringHistogram(NUM_HIST_BINS);
        }

    }

    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        myIOCOST = ioCostPerPage;

        //Start a new transaction
        Transaction myTransaction = new Transaction();
        myTransaction.start();

        //get the table's iterator
        HeapFile hf = (HeapFile)Database.getCatalog().myCatalog.get(tableid).file;
        myHeapFile = hf;
        DbFileIterator iterator = hf.iterator(myTransaction.getId());
        myTD = hf.getTupleDesc();
        int numFields = myTD.myTDItems.size();

        //each field in the table has a respective histogram
        tableHistograms = new HashMap<Integer, FieldHistogram>();

        //find the indices of the int fields, and add null FieldHistograms
        //to their mapping for now. for string histograms, instantiate
        //a string histogram
        Vector<Integer> intFieldIndices = new Vector<Integer>();
        for(int i = 0; i < numFields; i++){
            if(myTD.myTDItems.get(i).fieldType == Type.INT_TYPE){
                intFieldIndices.add(i);
                tableHistograms.put(i,null);
            }
            else{
                FieldHistogram fh = new FieldHistogram(0,0);
                fh.initSTRHist();
                tableHistograms.put(i, fh);
            }

        }

        int numIntFields = intFieldIndices.size();

        //iterate through all the int fields,
        //and find min/max values for each field
        //for the first tuple in the table, set the min/max 
        //to its values - for following tuples, compare with the
        //current min/max
        boolean firstTuple = true;

        try{

        iterator.rewind();
        while(iterator.hasNext()){
            Tuple t = iterator.next();
            for(int i = 0; i < numIntFields; i++){
                int fieldIndex = intFieldIndices.get(i);
                int curValue = ((IntField)t.getField(fieldIndex)).getValue();
                if(firstTuple){
                    tableHistograms.put(fieldIndex,new FieldHistogram(curValue,curValue));
                }
                else{
                    FieldHistogram fs = tableHistograms.get(fieldIndex);
                    if(curValue < fs.myMin){
                        fs.setMin(curValue);
                        tableHistograms.put(fieldIndex,fs);
                    }
                    else if(curValue > fs.myMax){
                        fs.setMax(curValue);
                        tableHistograms.put(fieldIndex,fs);
                    }
                }
            }
            firstTuple = false;
        }

        //now that we have min/values for each int field, instatiate the
        //int histograms
        for(int i = 0; i < numIntFields; i++){
            FieldHistogram fh = tableHistograms.get(intFieldIndices.get(i));
            fh.initINTHist();
            tableHistograms.put(intFieldIndices.get(i),fh);
        }

        //rewind the iterator, and start
        //adding values to the histograms
        iterator.rewind();
        numTuples = 0;
        while(iterator.hasNext()){
            numTuples++;
            Tuple t = iterator.next();
            for(int i = 0; i < numFields; i++){
                if(intFieldIndices.contains(i)){
                    int curValue = ((IntField)t.getField(i)).getValue();
                    FieldHistogram fs = tableHistograms.get(i);
                    fs.myIntHistogram.addValue(curValue);
                    tableHistograms.put(i,fs);
                }
                else{
                    String curValue = ((StringField)t.getField(i)).getValue();
                    FieldHistogram fs = tableHistograms.get(i);
                    fs.myStrHistogram.addValue(curValue);
                    tableHistograms.put(i,fs);
                }

            }
        }
        iterator.close();
    }catch(Exception e){
        System.out.print(e);
    }


    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return myHeapFile.numPages()*myIOCOST;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        
        if(myTD.myTDItems.get(field).fieldType == Type.INT_TYPE){
            IntHistogram ih = tableHistograms.get(field).myIntHistogram;
            return ih.estimateSelectivity(op,((IntField)constant).getValue());
        }
        else{
            StringHistogram sh = tableHistograms.get(field).myStrHistogram;
            return sh.estimateSelectivity(op,((StringField)constant).getValue());
        }
        
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}
