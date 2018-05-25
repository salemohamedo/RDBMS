package simpledb;

import java.util.*;
//import java.io.*;

public class MyTupleIterator<T> implements Iterator<Tuple> {

	public Vector<Tuple> myTupleVector;
	Iterator<Tuple> myTupleIterator;

	public MyTupleIterator(Vector<Tuple> tupleVector){
		myTupleVector = tupleVector;
		myTupleIterator = myTupleVector.iterator();
	}

	public Tuple next(){
		return myTupleIterator.next();
	}


	public boolean hasNext(){
		return myTupleIterator.hasNext();
	}

	public void remove() throws UnsupportedOperationException{
		throw new UnsupportedOperationException();
	}

}