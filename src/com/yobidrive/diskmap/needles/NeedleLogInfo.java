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
import java.util.Date;
/**
 * Class for managing cleaning info for cost/benefit needle log cleaning
 * @author Francois Vaille
 *
 */
public class NeedleLogInfo implements Comparable<NeedleLogInfo>, Cloneable{
	public static int INFOSIZE = 36 ;
	private long yougestDOB = 0L ; 		// For method of yougest date of birth
	private long averageDOB = 0L ; 		// For method of average datd of birth
	private long liveBytes = 0L ;
	private long deadBytes = 0L ;
	private boolean useAverage = true ;
	private int needleFileNumber = 0 ; // the inspected needle log
	
	public NeedleLogInfo(boolean useAverage) {
		this.useAverage = useAverage ;
	}
	
	public NeedleLogInfo clone() {
		NeedleLogInfo result = new NeedleLogInfo(useAverage) ;
		result.setYougestDOB(this.getYougestDOB()) ;
		result.setAverageDOB(this.getAverageDOB()) ;
		result.setLiveBytes(this.getLiveBytes()) ;
		result.setDeadBytes(this.getDeadBytes()) ;
		result.setNeedleFileNumber(this.getNeedleFileNumber()) ;
		return result ;
	}
	
	public void getNeedleLogInfo(ByteBuffer input) throws BufferUnderflowException {
		needleFileNumber = input.getInt() ;
		yougestDOB = input.getLong() ;
		averageDOB = input.getLong() ;
		liveBytes = input.getLong() ;
		deadBytes = input.getLong() ;
	}
	
	public int compareTo(NeedleLogInfo otherInfo) {
		if ( otherInfo == null )
			return 1 ; // Always cheaper than not existing
		float myBOC = getBenefitOnCost() ;
		float otherBOC = otherInfo.getBenefitOnCost() ;
		if ( myBOC < otherBOC )
			return -1 ;
		else if ( myBOC > otherBOC )
			return 1 ;
		else 
			return 0 ;
	}
	
	
	public void putNeedleLogInfo(ByteBuffer output) throws BufferOverflowException {
		putNeedleLogInfo(output,false) ;
	}
	
	public void putNeedleLogInfo(ByteBuffer output, boolean flip) throws BufferOverflowException {
		output.putInt(needleFileNumber) ;
		output.putLong(yougestDOB) ;
		output.putLong(averageDOB) ;
		output.putLong(liveBytes) ;
		output.putLong(deadBytes) ;	
		
		if ( flip )
			output.flip() ;
	}

	
	
	public long getYougestDOB() {
		return yougestDOB;
	}
	public void setYougestDOB(long yougestDOB) {
		this.yougestDOB = yougestDOB;
	}
	public long getAverageDOB() {
		return averageDOB;
	}
	public void setAverageDOB(long averageDOB) {
		this.averageDOB = averageDOB;
	}
	
	public int getNeedleFileNumber() {
		return needleFileNumber;
	}
	public void setNeedleFileNumber(int needleFileNumber) {
		this.needleFileNumber = needleFileNumber;
	}
	
	public void addLiveNeedle(long age, long weight) {
		if ( age > yougestDOB )
			yougestDOB = age ;
		averageDOB = ((liveBytes * averageDOB) / (liveBytes+weight)) + (age / (liveBytes+weight)) ;
		liveBytes += weight ;
	}
	
	public void addDeadNeedle(long weight) {
		liveBytes = liveBytes - weight ;
		if ( liveBytes < 0 )
			liveBytes = 0 ;
		deadBytes += weight ;
	}
	public long getLiveBytes() {
		return liveBytes;
	}
	public void setLiveBytes(long liveBytes) {
		this.liveBytes = liveBytes;
	}
	public long getDeadBytes() {
		return deadBytes;
	}
	public void setDeadBytes(long deadBytes) {
		this.deadBytes = deadBytes;
	}
	
	public float getBenefitOnCost() {
		float utilization = getUtilization() ;
		long age = new Date().getTime() - (useAverage?averageDOB:yougestDOB) ;
		return ((1-utilization)/(1+utilization))*age ;	
	}
	
	public float getUtilization() {
		long logSize = liveBytes + deadBytes ;
		if ( logSize ==  0 )
			return 0 ;
		return liveBytes / logSize ;
	}
	
}
