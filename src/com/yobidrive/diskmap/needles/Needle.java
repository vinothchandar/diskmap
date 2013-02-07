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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import voldemort.versioning.VectorClock;

/**
 * Created by EZC Group S.A.
 * @author Francois Vaille
 * 
 * Representation of a needle in log file
 *
 */
public  class Needle {
	private static Log logger = LogFactory.getLog(Needle.class);
	public static int NEEDLEHEADEROVERHEAD = 45 ;
	public static int NEEDLEFLAGOVERHEAD = 17 ;
	public static int NEEDLEPOSTDATAOVERHEAD = 20 ;
	public static int NEEDLEOVERHEAD = NEEDLEHEADEROVERHEAD + NEEDLEPOSTDATAOVERHEAD + 255 ;
	public static final int INTSIZE = 4 ;
	private static final int MAGICSTART = 0xBABECAFE;
	private static final int MAGICSTART_BADENDIAN = 0xFECABEBA ;
	private static final int MAGICEND = 0xABADFACE;
	private static final byte PADDING = 0x00;

	private long needleNumber ;
	private byte[] keyBytes = null ;
	private VectorClock version ;
	private byte flags = 0x00 ;
	private int originalFileNumber = -1 ;
	private int originalSize = 0 ;
	private int size = 0 ;
	private byte[] data ;
	
	
	NeedlePointer previousNeedle = null ; // Chaining to previous needle with same hash
	int readBytes = 0 ;
	



	
	public Needle() {
		
	}
	
	public boolean isDeleted() {
		return flags == (byte) 0x01 ;
	}
	
	public void setDeleted(boolean deleted) {
		flags = deleted?(byte)0x01:(byte)0x00 ;
	}
	
	/** Total size for writing, including the padding
	 * @return
	 */
	public int getWriteTotalSize() {
		return  NEEDLEOVERHEAD + (keyBytes==null?0:keyBytes.length) +  (version==null?0:version.toBytes().length)
			+ getSize() ;
	}
	
	public int getHeaderSize() {
		return NEEDLEHEADEROVERHEAD + (keyBytes==null?0:keyBytes.length) +  (version==null?0:version.toBytes().length) ;
	}
	
	/**  Total size excluding the end padding, for random access
	 * @return
	 */
	public int getTotalSizeFromData() {
		//return NEEDLEOVERHEAD + (keyBytes==null?0:keyBytes.length) +  (version==null?0:version.toBytes().length)
		//	+ getSizeFromData() ;
		return NEEDLEHEADEROVERHEAD + NEEDLEPOSTDATAOVERHEAD + (keyBytes==null?0:keyBytes.length) +  (version==null?0:version.toBytes().length)
			+ getSizeFromData() ;
	}
	
	/** Total size including the end padding, for access to the next needle
	 * @return
	 */
	public int getRoundedTotalSize() {
		//return NEEDLEOVERHEAD + (keyBytes==null?0:keyBytes.length) +  (version==null?0:version.toBytes().length)
		//	+ getSizeFromData() ;
		int result =  NEEDLEHEADEROVERHEAD + NEEDLEPOSTDATAOVERHEAD + (keyBytes==null?0:keyBytes.length) +  (version==null?0:version.toBytes().length)
			+ getSizeFromData() ;
		int extraBytes = (result % 256) ;
		return result + (extraBytes<=0?0:256-extraBytes) ;
	}
	
	public int getPostDataSize() {
		return NEEDLEPOSTDATAOVERHEAD + getSizeFromData() ;
	}
	
	
	public void putNeedleInBuffer(ByteBuffer result) throws Exception {
		int startPosition = result.position() ;
		result.limit(result.capacity()) ;
		result.putInt(MAGICSTART) ;
		result.putLong(needleNumber) ;
		result.put(flags) ;
		result.putInt(keyBytes.length) ;
		result.put(keyBytes) ;
		result.putInt(version==null?0:version.toBytes().length) ;
		if ( version != null )
			result.put(version.toBytes()) ;
		result.putInt(previousNeedle==null?-1:previousNeedle.getNeedleFileNumber()) ;		// Chaining
		result.putLong(previousNeedle==null?-1L:previousNeedle.getNeedleOffset()) ;	// Chaining
		result.putInt(originalFileNumber) ; // Original needle location (for cleaning)
		result.putInt(originalSize) ; // Original needle size (for cleaning)
		result.putInt(data==null?0:data.length) ;
		if ( data!= null )
			result.put(data) ;
		result.putInt(MAGICEND) ;
		result.put(hashMD5()) ;
		while ( ((result.position() - startPosition) % 256) > 0) {
			result.put(PADDING) ;
		}
		result.flip() ;
	}
	
	
	
	public boolean getNeedleFromBuffer(ByteBuffer input) throws Exception {
		if ( !getNeedleHeaderFromBuffer(input) )
			return false ;
		return getNeedleDataFromBuffer(input) ;
	}
	
	public boolean getNeedleDataFromBuffer(ByteBuffer input) throws Exception {
		try {
			// input.reset() ; // Back to end of header
			// int startPosition = readBytes * -1 ;
			
			if ( size > 0 ) {
				data = new byte[size] ;
				input.get(data) ;
			} else
				data = null ;
			int magic = input.getInt() ;
			if ( magic != MAGICEND ) {
				logger.error("No MAGICEND within needle "+this.needleNumber) ;
				return false ;
			}
			byte[] md5 = new byte[16] ;
			input.get(md5) ;
			byte [] currentMD5 = hashMD5() ;
			if ( !Arrays.equals(md5,currentMD5) ) {
				logger.error("Needle MD5 failed for "+new String(keyBytes,"UTF-8")) ;
				return false ;
			}
		} catch ( BufferUnderflowException bue ) {
			return false ;
		}
		return true ;
	}
	
	public boolean getNeedleHeaderFromBuffer(ByteBuffer input) throws Exception {
		try {
			// Reinit needle
			keyBytes = null ;
			version = null ;
			flags = 0x00 ;
			size = 0 ;
			data = null ;
			previousNeedle = null ; // Chaining
			readBytes = 0 ;
			// Processes reading
			input.rewind() ;
			int startPosition = input.position() ;
			int magic = input.getInt() ;
			if ( magic == MAGICSTART_BADENDIAN ) {
				if ( input.order().equals(ByteOrder.BIG_ENDIAN) )
					input.order(ByteOrder.LITTLE_ENDIAN);
				else
					input.order(ByteOrder.BIG_ENDIAN);
			} else if ( magic != MAGICSTART ) {
				logger.error("Buffer not starting with needle") ;
				return false ;
			}
			needleNumber = input.getLong() ;
			flags = input.get() ;
			int keyLen = input.getInt() ;
			if ( keyLen > 2028 ) {
				logger.error("Crazy needle key len") ;
				return false ;
			}
			keyBytes = new byte[keyLen] ;
			input.get(keyBytes) ;
			int versionLen = input.getInt() ;
			if ( versionLen > 1024 * 16 ) {
				logger.error("Crazy needle version len") ;
				return false ;
			}
			if ( versionLen == 0)
				version = null ;
			else {
				byte[] versionBytes = new byte[versionLen] ;
				input.get(versionBytes) ;
				version = new VectorClock(versionBytes) ;
			}
			int previousLogNumber = input.getInt();		// Chaining
			long previousNeedleOffset = input.getLong();	// Chaining
			if ( previousLogNumber != -1 && previousNeedleOffset != -1L ) {
				previousNeedle = new NeedlePointer() ;
				previousNeedle.setNeedleFileNumber(previousLogNumber) ;
				previousNeedle.setNeedleOffset(previousNeedleOffset) ;
			}
			originalFileNumber = input.getInt() ; // Original needle location (for cleaning)
			originalSize = input.getInt() ; // Original needle size (for cleaning)
			size = input.getInt() ;
			
			readBytes = input.position() - startPosition ;
			input.rewind() ;
			// input.mark() ;
			return true ;
		} catch ( BufferUnderflowException bue ) {
			return false ;
		}
	}
	
	
	public byte[] hashMD5() throws Exception {
		
		MessageDigest md = null;
		md = MessageDigest.getInstance("MD5");
		md.update(keyBytes) ;
		if ( version != null )
			md.update(version.toBytes()) ;
		md.update(flags) ;
		if ( data != null && data.length > 0 )
			md.update(data) ;
		byte[] v = md.digest() ;
		md.reset();
		return v ;
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

	public byte[] getData() {
		return data;
	}
	
	public byte[] getNotNullData() {
		return data==null?new byte[0]:data;
	}

	public void setData(byte[] data) {
		this.data = data;
		if ( data != null )
			size =  data.length ;
		else 
			size = 0;
	}



	public int getSize() {
		if ( data != null )
			return data.length ;
		else 
			return 0;
	}
	
	public int getSizeFromData() {
		return size ;
	}



	public NeedlePointer getPreviousNeedle() {
		return previousNeedle;
	}



	public void setPreviousNeedle(NeedlePointer previousNeedle) {
		this.previousNeedle = previousNeedle;
	}
	
	public NeedleHeader getNeedleHeader(NeedlePointer needleAddress) {
		NeedleHeader cnh = new NeedleHeader() ;
		cnh.setKeyBytes(this.getKeyBytes()==null?null:this.getKeyBytes().clone()) ;
		cnh.setVersion(this.getVersion()==null?null:this.getVersion().clone()) ;
		cnh.setFlags(this.getFlags()) ;
		cnh.setPreviousNeedle(previousNeedle==null?null:this.getPreviousNeedle().clone()) ;
		cnh.setNeedleAddress(needleAddress==null?null:needleAddress.clone()) ;
		cnh.setRoundedTotalSize(this.getRoundedTotalSize()) ;
		return cnh ;
	}

	public long getNeedleNumber() {
		return needleNumber;
	}

	public void setNeedleNumber(long needleNumber) {
		this.needleNumber = needleNumber;
	}

	public int getOriginalFileNumber() {
		return originalFileNumber;
	}

	public void setOriginalFileNumber(int originalFileNumber) {
		this.originalFileNumber = originalFileNumber;
	}

	public int getOriginalSize() {
		return originalSize;
	}

	public void setOriginalSize(int originalSize) {
		this.originalSize = originalSize;
	}
}
