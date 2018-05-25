package simpledb;

import static org.junit.Assert.assertEquals;

import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import junit.framework.JUnit4TestAdapter;
import simpledb.systemtest.SimpleDbTestBase;

public class IntegerAggregatorTest extends SimpleDbTestBase {

  int width1 = 2;
  DbIterator scan1, scan2;
  int[][] sum = null;
  int[][] min = null;
  int[][] max = null;
  int[][] avg = null;
  Object[][] sumString = null;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleList() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1,
        new int[] { 1, 2,
                    1, 4,
                    1, 6,
                    3, 2,
                    3, 4,
                    3, 6,
                    5, 7 });
    
    this.scan2 = TestUtil.createTupleList(width1,
            new Object[] { 
            			"adam", 2,
                        "adam", 4,
                        "frank", 6,
                        "robert", 2,
                        "frank", 4,
                        "john", 6,
                        "adam", 7 });

    // verify how the results progress after a few merges
    this.sum = new int[][] {
      { 1, 2 },
      { 1, 6 },
      { 1, 12 },
      { 1, 12, 3, 2 }
    };
    
    this.sumString = new Object[][] {
        { "adam", 2 },
        { "adam", 6 },
        { "adam", 6, "frank", 6 },
        { "adam", 6, "frank", 6, "robert", 2},
        { "adam", 6, "frank", 10, "robert", 2},
        { "adam", 6, "frank", 10, "robert", 2, "john", 6},
        { "adam", 13, "frank", 10, "robert", 2, "john", 6}
      };

    this.min = new int[][] {
      { 1, 2 },
      { 1, 2 },
      { 1, 2 },
      { 1, 2, 3, 2 }
    };

    this.max = new int[][] {
      { 1, 2 },
      { 1, 4 },
      { 1, 6 },
      { 1, 6, 3, 2 }
    };

    this.avg = new int[][] {
      { 1, 2 },
      { 1, 3 },
      { 1, 4 },
      { 1, 4, 3, 2 }
    };
  }

  /**
   * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over a sum
   */
  @Test public void mergeSum() throws Exception {
    scan1.open();
    IntegerAggregator agg = new IntegerAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.SUM);
    
    for (int[] step : sum) {
      agg.mergeTupleIntoGroup(scan1.next());
      DbIterator it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }
  
  /**
   * Test IntegerAggregator with grouping by strings.
   */
  @Test public void mergeSumGroupByString() throws Exception {
    scan2.open();
    IntegerAggregator agg = new IntegerAggregator(0, Type.STRING_TYPE, 1, Aggregator.Op.SUM);
    
    for (Object[] step : sumString) {
      agg.mergeTupleIntoGroup(scan2.next());
      DbIterator it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }

  /**
   * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over a min
   */
  @Test public void mergeMin() throws Exception {
    scan1.open();
    IntegerAggregator agg = new IntegerAggregator(0,Type.INT_TYPE,  1, Aggregator.Op.MIN);

    DbIterator it;
    for (int[] step : min) {
      agg.mergeTupleIntoGroup(scan1.next());
      it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }

  /**
   * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over a max
   */
  @Test public void mergeMax() throws Exception {
    scan1.open();
    IntegerAggregator agg = new IntegerAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.MAX);

    DbIterator it;
    for (int[] step : max) {
      agg.mergeTupleIntoGroup(scan1.next());
      it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }

  /**
   * Test IntegerAggregator.mergeTupleIntoGroup() and iterator() over an avg
   */
  @Test public void mergeAvg() throws Exception {
    scan1.open();
    IntegerAggregator agg = new IntegerAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.AVG);

    DbIterator it;
    for (int[] step : avg) {
      agg.mergeTupleIntoGroup(scan1.next());
      it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }

  /**
   * Test IntegerAggregator.iterator() for DbIterator behaviour
   */
  @Test public void testIterator() throws Exception {
    // first, populate the aggregator via sum over scan1
    scan1.open();
    IntegerAggregator agg = new IntegerAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.SUM);
    try {
      while (true)
        agg.mergeTupleIntoGroup(scan1.next());
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }

    DbIterator it = agg.iterator();
    it.open();

    // verify it has three elements
    int count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, count);

    // rewind and try again
    it.rewind();
    count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, count);

    // close it and check that we don't get anything
    it.close();
    try {
      it.next();
      throw new Exception("IntegerAggregator iterator yielded tuple after close");
    } catch (Exception e) {
      // explicitly ignored
    }
  }
  
  /**
   * Test Integer Aggregator with grouping by String.
   */
  @Test public void testIteratorString() throws Exception {
    // first, populate the aggregator via sum over scan2
    scan2.open();
    IntegerAggregator agg = new IntegerAggregator(0, Type.STRING_TYPE, 1, Aggregator.Op.SUM);
    try {
      while (true)
        agg.mergeTupleIntoGroup(scan2.next());
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }

    DbIterator it = agg.iterator();
    it.open();

    // verify it has four elements
    int count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(4, count);
  }


  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegerAggregatorTest.class);
  }
}

