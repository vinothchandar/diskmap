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

package com.yobidrive.diskmap.buckets;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.yobidrive.diskmap.needles.NeedleLogInfo;
import com.yobidrive.diskmap.needles.NeedlePointer;



public class BucketTable {
	ByteBuffer[] tableBuffers = null ;
	ReentrantReadWriteLock[] locks = null ;
	ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock() ;
	int nbBuffers = 0 ;
	int bucketsPerBuffer = 0 ;
	volatile NeedlePointer lastCheckPoint = null ;
	volatile NeedlePointer requestedCheckPoint ;
	public HashMap<Long, NeedlePointer> dirtyTable = null ;
	NeedleLogInfo dd ;
	
	
	public BucketTable(int nbBuffers, int bucketsPerBuffer) {
		this.nbBuffers = nbBuffers ;
		this.bucketsPerBuffer = bucketsPerBuffer ;
		tableBuffers = new ByteBuffer[nbBuffers] ;
		locks = new ReentrantReadWriteLock[nbBuffers] ;
		for ( int i = 0 ; i < nbBuffers; i++) {
			tableBuffers[i] = ByteBuffer.allocateDirect(8*bucketsPerBuffer) ;
			locks[i] = new ReentrantReadWriteLock(true) ;
		}
		dirtyTable = new HashMap<Long, NeedlePointer>(4000000) ; // Starts with 4M entries
	}
	
	public void put(long bucket, NeedlePointer needlePointer) throws BucketTableException {
		int numBuffer = (int)(bucket /  bucketsPerBuffer) ;
		boolean shouldThrow = false ;
		commitLock.readLock().lock();
		locks[numBuffer].writeLock().lock() ;
		try {
			// If the new needle if before the last checkpoint, throw exception
			if ( lastCheckPoint != null && needlePointer.compareTo(lastCheckPoint) <= 0 )
				shouldThrow = true ;
			// If the new needle is after the requested checkpoint put in dirty cache
			if ( requestedCheckPoint!= null && needlePointer.compareTo(requestedCheckPoint) > 0 ) {
				dirtyTable.put(new Long(bucket), needlePointer) ;
			} else {
				// Not checkpointed yet, so direct write to table
				int bucketInBuffer = (int) bucket %  bucketsPerBuffer ;
				putInBuffer(numBuffer, bucketInBuffer, needlePointer) ;
			}
		} catch ( Throwable th ) {
			throw new BucketTableException("Error putting "+needlePointer.toString()+" at bucket "+bucket,th) ;
		} finally {
			locks[numBuffer].writeLock().unlock() ;
			commitLock.readLock().unlock();
		}
		if ( shouldThrow )
			throw new BucketTableException(needlePointer.toString()+" for bucket "+bucket+" before last checkpoint "+lastCheckPoint) ;
	}
	
	private void putInBuffer(int numBuffer, int bucketInBuffer, NeedlePointer needlePointer) throws BucketTableException {
		int offsetInBuffer = bucketInBuffer*8 ;
		needlePointer.putNeedlePointerToBuffer(tableBuffers[numBuffer], (int) offsetInBuffer)  ;
	}
	
	public NeedlePointer get(long bucket) throws BucketTableException {
		int numBuffer = (int)(bucket / bucketsPerBuffer ) ;
		NeedlePointer result = null ;
		NeedlePointer cache = null ;
		locks[numBuffer].readLock().lock() ;
		try {
			// First looks in dirty cache
			Long key = new Long(bucket) ;
			if ( dirtyTable.containsKey(key) ) {
				cache = dirtyTable.get(new Long(bucket)) ; 
			}
			int bucketInBuffer = (int) bucket %  bucketsPerBuffer ;
			result = getFromBuffer(numBuffer, bucketInBuffer) ;
			if ( cache != null && cache.compareTo(result) > 0 )
				result = cache ;
		} catch ( Throwable th ) {
				throw new BucketTableException("Error getting from bucket "+bucket,th) ;
		} finally {
			locks[numBuffer].readLock().unlock() ;
		}
		return result ;
	}
	
	public void transferDirtyEntriesToLiveTableBuffer() {
		for ( int numBuffer = 0 ; numBuffer < nbBuffers; numBuffer++ ) {
			long ts = new Date().getTime() ;
			locks[numBuffer].writeLock().lock() ;
			try {
				for ( int i = 0 ; i < bucketsPerBuffer; i++ ) {
					long bucket = (bucketsPerBuffer*numBuffer) + i ;
					Long key = new Long(bucket) ;
					if ( dirtyTable.containsKey(key) ) {
						NeedlePointer currentNeedlePointer = getFromBuffer(numBuffer, i) ;
						NeedlePointer cachedNeedlePointer = dirtyTable.remove(key) ;
						if ( cachedNeedlePointer.compareTo(currentNeedlePointer) > 0 )
							putInBuffer(numBuffer, i, cachedNeedlePointer)  ;
					}
				}
			} catch ( Throwable th ) {
				throw new BucketTableException("Error transferring dirty buckets",th) ;
			} finally {
				locks[numBuffer].writeLock().unlock() ;
			}
	 		long lap = new Date().getTime() - ts ;
	 		// System.out.println("Copied "+bucketsPerBuffer+" buckets in "+lap+" ms") ;
		}
	}
	
	public void format() {
		NeedlePointer emptyNeedlePointer = new NeedlePointer() ;
		for ( int numBuffer = 0 ; numBuffer < nbBuffers; numBuffer++ ) {
			long ts = new Date().getTime() ;
			

			for ( int i = 0 ; i < bucketsPerBuffer; i++ ) {
				putInBuffer(numBuffer, i, emptyNeedlePointer)  ;
			}
		
	 		long lap = new Date().getTime() - ts ;
		}
	}
	
	public void signalsCheckPointDoneAndCleanDirtyEntries() {
		signalsCheckPointDone() ;
		transferDirtyEntriesToLiveTableBuffer() ;
	}
	
	public void signalsCheckPointDone() {
		commitLock.writeLock().lock();
		lastCheckPoint = requestedCheckPoint ;
		requestedCheckPoint = null ;
		commitLock.writeLock().unlock();
	}
	
	public void requestCheckPoint(NeedlePointer checkPoint) {
		commitLock.writeLock().lock();
		requestedCheckPoint = checkPoint ;
		commitLock.writeLock().unlock();
	}
	
	public void setInitialCheckPoint(NeedlePointer checkpoint) {
		commitLock.writeLock().lock();
		lastCheckPoint = checkpoint ;
		requestedCheckPoint = null ;
		commitLock.writeLock().unlock();
	}
	
	public NeedlePointer getLastCheckPoint() {
		NeedlePointer result ;
		commitLock.readLock().lock();
		result = lastCheckPoint.clone() ;
		commitLock.readLock().unlock();
		return result ;
	}
	
	public NeedlePointer getRequestedCheckPoint() {
		NeedlePointer result ;
		commitLock.readLock().lock();
		result = requestedCheckPoint.clone() ;
		commitLock.readLock().unlock();
		return result ;
	}

	
	private NeedlePointer getFromBuffer(int numBuffer, int bucketInBuffer) throws BucketTableException {
		int offsetInBuffer = bucketInBuffer*8 ;
		NeedlePointer needlePointer = new NeedlePointer() ;
		needlePointer.getNeedlePointerFromBuffer(tableBuffers[numBuffer], (int) offsetInBuffer) ;
		return needlePointer ;
	}
	
	public ByteBuffer getBuffer(int bufferNumber) throws BucketTableException {
		return tableBuffers[bufferNumber] ;
	}
	
	public void prepareBufferForReading(int bufferNumber) throws BucketTableException {
		tableBuffers[bufferNumber].rewind() ;
		tableBuffers[bufferNumber].limit(8*bucketsPerBuffer) ;
	}
	
	public void prepareBufferForWriting(int bufferNumber) throws BucketTableException {
		tableBuffers[bufferNumber].rewind() ;
	}
	
	public int getNbBuffers() {
		return nbBuffers ;
	}
	
	public int getDirtyBuckets() {
		return dirtyTable==null?0:dirtyTable.size() ;
	}
	
}
