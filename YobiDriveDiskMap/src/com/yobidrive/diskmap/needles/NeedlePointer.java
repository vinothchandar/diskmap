/*
 * Copyright 2012-2013 EZC Group S.A. RCS Luxembourg B140949
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 */

package com.yobidrive.diskmap.needles;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import voldemort.versioning.VectorClock;

/**
 * Created by EZC Group S.A.
 * @author Francois Vaille
 *
 */
public class NeedlePointer implements Cloneable, Comparable<NeedlePointer> {
	public static int POINTERSIZE = 4 + 4 ;
	private int needleFileNumber = -1 ;
	private long needleOffset = -1L ;
	private boolean isSingle = false ;
	
	public static int compareNullablePointers(NeedlePointer arg1, NeedlePointer arg2) {
		final int BEFORE = -1;
	    final int EQUAL = 0;
	    final int AFTER = 1;
	    
	    if ( arg1 == null ) {
	    	if ( arg2 == null )
	    		return EQUAL ;
	    	else if ( arg2.isEmpty() )
	    		return EQUAL ;
	    	else
	    		return BEFORE ;
	    } else if ( arg2 == null ) {
	    	return AFTER ;
	    } else
	    	return arg1.compareTo(arg2) ;
	}
	
	public NeedlePointer clone() {
		NeedlePointer newNeedle = new NeedlePointer() ;
		newNeedle.setNeedleFileNumber(this.getNeedleFileNumber()) ;
		newNeedle.setNeedleOffset(this.getNeedleOffset()) ;
		newNeedle.setSingle(this.isSingle()) ;
		return newNeedle ;
	}
	
	public NeedlePointer getSingleFormattedClone() {
		if ( !isSingle() ) 
			return clone() ;
		else {
			NeedlePointer newNeedle = this.clone() ;
			if ( newNeedle.getNeedleOffset() > 0 )
				newNeedle.setNeedleOffset(this.getNeedleOffset()*-1) ;
			return newNeedle ;
		}
	}
	
	
	public NeedlePointer positionToNextFile() {
		if ( this.isEmpty() )
			return null ;
		needleOffset = 0L ;
		needleFileNumber++ ;
		return this ;
	}
	
	public void positionToNextNeedle(long position) {
		if ( this.isEmpty() ) {
			throw new NeedleManagerException("Trying to do a position to next needle from an empty Needle Pointer") ;
		} else {		
			needleOffset = position ;
		}
	}
	
	public void copyFrom(NeedlePointer needlePointer) {
		this.setNeedleFileNumber(needlePointer.getNeedleFileNumber()) ;
		this.setNeedleOffset(needlePointer.getNeedleOffset()) ;
	}
	
	public int getNeedleFileNumber() {
		return needleFileNumber;
	}
	
	public String getFormattedNeedleFileNumber() {
		return getFormattedNeedleFileNumber(needleFileNumber);
	}
	
	public static String getFormattedNeedleFileNumber(int needleFileNumber) {
		if ( needleFileNumber == -1 )
			return null ;
		String formatted = Integer.toHexString(needleFileNumber) ;
		while ( formatted.length() < 8 )
			formatted = "0"+formatted ;
		return formatted;
	}
	
	public void setNeedleFileNumber(int needleFileNumber) {
		this.needleFileNumber = needleFileNumber;
	}
	public long getNeedleOffset() {
		return needleOffset;
	}
	public String getFormattedNeedleOffset() {
		if ( needleOffset == -1L )
			return null ;
		return Long.toHexString(needleOffset) ;
	}
	
	public void setNeedleOffset(long needleOffset) {
		this.needleOffset = needleOffset;
	}
	
	public void beginningOfLogFile() {
		this.needleOffset = 0L;
	}
	
	public void getNeedlePointerFromBuffer(ByteBuffer input) throws BufferUnderflowException {
		// Reinit needle
		needleFileNumber = input.getInt() ;
		int realOffset = input.getInt() ;
		getOffsetAndSingleFlag(realOffset) ;
	}
	
	private void getOffsetAndSingleFlag(int myOffset) throws BufferUnderflowException {
		if ( myOffset == -1 ) {
			needleOffset = -1 ;
			setSingle(false) ;
		} else if ( myOffset == -2 ) {
			needleOffset = 0 ;
			setSingle(true) ;
		} else if ( myOffset < 0 ){
			needleOffset = myOffset * -256 ;
			setSingle(true) ;
		} else {
			needleOffset = myOffset * 256 ;
			setSingle(false) ;
		}
	}
	
	public void getNeedlePointerFromBuffer(ByteBuffer input, int position) throws BufferUnderflowException {
		// Reinit needle
		needleFileNumber = input.getInt(position) ;
		int realOffset = input.getInt(position+4) ;
		getOffsetAndSingleFlag(realOffset) ;
	}
	
	public void putNeedlePointerToBuffer(ByteBuffer output) throws BufferOverflowException {
		putNeedlePointerToBuffer(output,false) ;
	}
	
	public void putNeedlePointerToBuffer(ByteBuffer output, boolean flip) throws BufferOverflowException {
		output.putInt(needleFileNumber) ;
		if ( needleOffset == -1L ) 
			output.putInt((int) -1) ;
		else {
			long computedOffset = needleOffset / 256 ;
			if ( needleOffset % 256 != 0 )
				throw new NeedleManagerException("Needle offset is not a multiple of 256 ") ;
			if (computedOffset > Integer.MAX_VALUE )
				throw new NeedleManagerException("Needle offset exceeds "+Integer.MAX_VALUE *256) ;
			output.putInt((int) computedOffset) ;
		}
		if ( flip )
			output.flip() ;
	}
	
	public void putNeedlePointerToBuffer(ByteBuffer output, int position) throws BufferOverflowException {
		output.putInt(position, needleFileNumber) ;
		if ( needleOffset == -1L ) 
			output.putInt(position+4, (int) -1) ;
		else {
			long computedOffset = needleOffset / 256 ;
			if ( needleOffset % 256 != 0 )
				throw new NeedleManagerException("Needle offset is not a multiple of 256 ") ;
			if (computedOffset > Integer.MAX_VALUE )
				throw new NeedleManagerException("Needle offset exceeds "+Integer.MAX_VALUE *256) ;
			output.putInt(position+4, (int) computedOffset) ;
		}
	}
	
	public boolean isEmpty() {
		return ((needleFileNumber == -1) && (needleOffset == -1L)) ;
	}
	
	public void setEmpty() {
		needleFileNumber = -1 ;
		needleOffset = -1L ;
	}
	
	/**
	  * @param aThat is a non-null Account.
	  *
	  * @throws NullPointerException if aThat is null.
	  */
	  public int compareTo( NeedlePointer aThat ) {
	    final int BEFORE = -1;
	    final int EQUAL = 0;
	    final int AFTER = 1;

	    //this optimization is usually worthwhile, and can
	    //always be added
	    if ( this == aThat ) return EQUAL;

	    //primitive numbers follow this form
	    if (this.needleFileNumber < aThat.needleFileNumber) return BEFORE;
	    if (this.needleFileNumber > aThat.needleFileNumber) return AFTER;

	    if (this.needleOffset < aThat.needleOffset) return BEFORE;
	    if (this.needleOffset > aThat.needleOffset) return AFTER;

	    return EQUAL;
	  }
	  
	  @Override
	  public String toString() {
		  return "Needle "+Integer.toHexString(needleFileNumber)+" / "+Long.toString(needleOffset) ;
	  }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + needleFileNumber;
		result = prime * result + (int) (needleOffset ^ (needleOffset >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final NeedlePointer other = (NeedlePointer) obj;
		if (needleFileNumber != other.needleFileNumber)
			return false;
		if (needleOffset != other.needleOffset)
			return false;
		return true;
	}

	public boolean isSingle() {
		return isSingle;
	}

	public void setSingle(boolean isSingle) {
		this.isSingle = isSingle;
	}
	  
	  
}
