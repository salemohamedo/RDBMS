package simpledb;
import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */


    public int numBuckets;
    public int myMin;
    public int myMax;
    public int myWidth;
    public int numTuples;

    public int[] myHistogram;

    public IntHistogram(int buckets, int min, int max) {
        numBuckets = buckets;
        numTuples = 0;
        myMin = min;
        myMax = max;
        //range of a given bucket
        myWidth =  (int)Math.ceil(((double)max - min)/buckets);
        myHistogram = new int[numBuckets];
        Arrays.fill(myHistogram,0);

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int bucket;
        if(v == myMin){
            bucket = 0;
        }
        else if(v == myMax){
            bucket = numBuckets -1;
        }
        else{
            bucket = findBucket(v);
        }
    	myHistogram[bucket]++;
        numTuples++;
    }

    //helper method to find index of bucket
    public int findBucket(int v){
        return (v-myMin)/myWidth;
    }

    //method to calculate sum of heights of adjacent buckets in either direction
    //as specificed by the the boolean
    public int rangeSum(int bucket, boolean right){
        int sum = 0;
        if(right){
            for(int i = bucket + 1; i < myHistogram.length; i++){
                sum += myHistogram[i];
            }
        }
        else{
            for(int i = bucket - 1; i >= 0; i--){
                sum += myHistogram[i];
            }
        }
        return sum; 
    }


    //selectivity for equals op
    public double equalsSelectivity(int bucket){
        if(bucket < 0 || bucket >= findBucket(myMax)){
            return 0.0;
        }
        return (double)((double)myHistogram[bucket]/myWidth)/numTuples;
    }

    //selectivity for greater/less than op
    public double rangeSelectivity(int bucket, int v, boolean greater){
        if(bucket < 0){
            if(greater){
                return 1.0;
            }
            else{
                return 0.0;
            }
        }
        if(bucket >= findBucket(myMax)){
            if(greater){
                return 0.0;
            }
            else{
                return 1.0;
            }
        }
        int rangeSum = rangeSum(bucket,greater);
        double bucketSelectivity;
        if(greater){
            bucketSelectivity = (double)(((bucket+1)*myWidth)+myMin-v)/myWidth;
        }
        else{
            bucketSelectivity = (double)(v+myMin-((bucket-1)*myWidth))/myWidth; 
        }

        double fractionOfBucket = (double)(bucketSelectivity*myHistogram[bucket])/numTuples;
        return (fractionOfBucket + rangeSum)/numTuples;     

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */

    public double estimateSelectivity(Predicate.Op op, int v) {

    	int targetBucket = findBucket(v);

        if(op == Predicate.Op.EQUALS){
                return equalsSelectivity(targetBucket);
            }
        else if(op == Predicate.Op.GREATER_THAN){          
                return rangeSelectivity(targetBucket,v, true);
            }
        else if(op == Predicate.Op.LESS_THAN){
                return rangeSelectivity(targetBucket, v, false);
            }
        else if(op == Predicate.Op.NOT_EQUALS){
                return (double)(1 - equalsSelectivity(targetBucket));
            }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ){
                return rangeSelectivity(targetBucket, v, true) + equalsSelectivity(targetBucket);
            }
        else if(op == Predicate.Op.LESS_THAN_OR_EQ){
                return rangeSelectivity(targetBucket, v, false) + equalsSelectivity(targetBucket);       
            }      
        else{
                return -1.0;
            }               
        
    }

    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String histDesc = new String("");
        histDesc += "Integer Histogram with " + numTuples + " datapoints in " + numBuckets + " buckets\n";
        histDesc += "Min Value: " + myMin + " Max Value: " + myMax + "\n";
        return histDesc;
    }
}
