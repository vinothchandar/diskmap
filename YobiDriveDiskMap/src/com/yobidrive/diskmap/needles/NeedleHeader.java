/*
 * Copyright 2011-2013 EZC Group S.A. RCS Luxembourg B140949
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



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import voldemort.versioning.VectorClock;


/**
 * Internal Representation of a needle in log file
 * 
 * Created by EZC Group S.A.
 * @author Francois Vaille
 * 
 * 
 *
 */
public  class NeedleHeader {
	private static Log logger = LogFactory.getLog(NeedleHeader.class);
	private byte[] keyBytes = null ;
	private VectorClock version ;
	private byte flags = 0x00 ;
	// private int size = 0 ;
	// private byte[] data ;
	private NeedlePointer needleAddress = null ; // Our own address to chain to needle data
	private NeedlePointer previousNeedle = null ; // Chaining to previous needle with same hash
	private int roundedTotalSize = 0 ;
	

	public int getRoundedTotalSize() {
		return roundedTotalSize;
	}

	public void setRoundedTotalSize(int roundedTotalSize) {
		this.roundedTotalSize = roundedTotalSize;
	}

	public boolean isDeleted() {
		return flags == (byte) 0x01 ;
	}
	
	public void setDeleted(boolean deleted) {
		flags = deleted?(byte)0x01:(byte)0x00 ;
	}

	
	public NeedleHeader() {
		
	}

	public byte[] getKeyBytes() {
		return keyBytes;
	}
	
	public boolean isNull() {
		return keyBytes == null ;
	}
	
	public String getKey() {
		String key = null ;
		if ( keyBytes == null )
			return null ;
		try {
			key = new String(keyBytes,"UTF-8") ;
		} catch ( Throwable th ) {
			key = new String(keyBytes) ;
		}
		return key ;
	}

	public void setKeyBytes(byte[] keyBytes) {
		this.keyBytes = keyBytes;
	}
	
	public void setKey(String key) {
		if ( key == null || "".equals(key) )
			keyBytes = null ;
		else {
			try {
				keyBytes = key.getBytes("UTF-8") ;
			} catch ( Throwable th ) {
				keyBytes = key.getBytes() ;
			}
		}
	}

	public VectorClock getVersion() {
		return version;
	}

	public void setVersion(VectorClock version) {
		this.version = version;
	}

	public byte getFlags() {
		return flags;
	}

	public void setFlags(byte flags) {
		this.flags = flags;
	}

	public NeedlePointer getPreviousNeedle() {
		return previousNeedle;
	}

	public void setPreviousNeedle(NeedlePointer previousNeedle) {
		this.previousNeedle = previousNeedle;
	}

	public NeedlePointer getNeedleAddress() {
		return needleAddress;
	}

	public void setNeedleAddress(NeedlePointer needleAddress) {
		this.needleAddress = needleAddress;
	}
 
}
