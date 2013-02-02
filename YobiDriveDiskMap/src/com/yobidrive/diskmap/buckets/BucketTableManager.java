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
package com.yobidrive.diskmap.buckets;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yobidrive.diskmap.needles.NeedleLogInfo;
import com.yobidrive.diskmap.needles.NeedleManagerException;
import com.yobidrive.diskmap.needles.NeedlePointer;


/**
 * Created by EZC Group S.A.
 * @author Francois Vaille
 * 
 * Manager for disk based associative array for Chained hashmap
 * Each entry contains a needlePointer to the first chained needle. 
 * Chain must be walked thru to proper index in case of hash collision
 * To keep speed optimum (less than 2 needle reads), ideally choose a load factor <= 0.75
 * Space for bucket table file is 16 + nb_of_buckets * 8
 * Typical configuration: to manage a storage pod of 128TB with 64KB filechunks, 2 G Keys are required => 16GB
 * in this case a 128GB SSD disk would be more than optimum 
 *
 */
public class BucketTableManager {
	

	private static final int MAGICSTART = 0xBABECAFE;
	private static final int MAGICSTART_BADENDIAN = 0xFECABEBA ;
	private static final int MAGICEND = 0xABADFACE;
	private static final int INTSIZE = 4;
	public static final int HEADERSIZE = 16 + NeedlePointer.POINTERSIZE ;
	private static final int CHECKPOINTOFFSET = 12 ;
	private static final int SHUTTLEWAIT = 100 ; // 100ms
	// private static enum InitializeError {  OK, INCORRECTHEADER, CHANGEDMAPSIZE, INCORRECTFILESIZE, NOCHANNEL, FORMAT, FILESYSTEM, REPAIRREQUIRED  } ;
	// private static final int MAPPEDBUCKETS = 10000 ;
	private static final String EXTENSION = ".ydi" ;// Commited YobiDrive Diskmap Index
	private static final String UEXTENSION = ".ydu" ;// Uncommited YobiDrive Diskmap Index
	private BucketShuttleLauncher bucketShuttleLauncher = null ;
	// private final String FILENAME = "buckettable.ydm" ; // Disk Map Key File
	
	ByteOrder byteOrder = ByteOrder.BIG_ENDIAN ;
    String keyDirPath ; 						// Key file path
    String mode ;							// File write mode
    private File tableDir = null ;
    // private File tableFile = null ;
    long mapSize = 0L ;// BucketFNVHash.MAX_INT ; // Nb of entries in associative array
    boolean useAverage = true ;
    private static Log logger = LogFactory.getLog(BucketTableManager.class);

    // private BlockingQueue<ByteBuffer> readThreadBufferQ = null ;
    // private FileChannel tableChannel = null ;
    // private NeedlePointer lastCheckPoint = new NeedlePointer() ;
    // private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(NeedlePointer.POINTERSIZE) ;
    // private ByteBuffer transferBuffer = ByteBuffer.allocateDirect(NeedlePointer.POINTERSIZE*MAPPEDBUCKETS) ;
    // private LoadingCache<Long, NeedlePointer> bucketReadCache = null ;
    
    // public ConcurrentSkipListMap<Long, NeedlePointer> bucketDirtyCache = null ;
    // private int readThreads = 8 ;
    
    private BucketTable bucketTable = null ;
    private ConcurrentHashMap<Integer, NeedleLogInfo> logInfoPerLogNumber = null ;

    
    private int period = SHUTTLEWAIT; // 1s
    private long syncPeriod = 3000; // 3s
    
    // private BucketShuttle bucketShuttle = null ;
    
    BucketFNVHash bucketFNVHash = null ;
    // private long currentFileNumber = 0L ;
    private int nbBuffers = 0 ;
    private int entriesPerBuffer = 0 ;
   
    
    
	
	private long maxCycleTimePass1 = 0L ;
	private long maxCycleTimePass2 = 0L ;
	private Object cycleTimeLock = new Object() ;
	private Object cyclingLock = new Object() ;
    
   

	public BucketTableManager(String keyDirPath, int nbBuffers, int entriesPerBuffer, long syncPeriod, boolean useAverage) throws BucketTableManagerException {
        this.keyDirPath = keyDirPath;
        this.nbBuffers = nbBuffers ;
        this.entriesPerBuffer = entriesPerBuffer ;
        this.mapSize = nbBuffers * entriesPerBuffer ;
        this.period = 100 ;
        this.useAverage = useAverage ;
        // this.readThreads = readThreads ;	
        this.syncPeriod = syncPeriod ;
	}
	
	public void initialize() throws BucketTableManagerException {
		// Creates the hashing function
        bucketFNVHash = new BucketFNVHash(this.mapSize) ;
        
        // Creates the directory if required. bucket table should be on another drive than logs, typically a SSD drive
        tableDir = new File(keyDirPath) ;
        if ( !tableDir.exists() ) {
        	tableDir.mkdir() ;
        }
        if ( !tableDir.exists() || !tableDir.isDirectory() ) {
        	logger.error("Wrong directory "+keyDirPath) ;
        	throw new BucketTableManagerException() ;
        }
        // Create memory index
        bucketTable = new BucketTable(nbBuffers, entriesPerBuffer) ;
        logInfoPerLogNumber = new ConcurrentHashMap<Integer, NeedleLogInfo>() ;
		
        // Open channel if file exist and gets checkpoint
        initializeBucketTableFromLastCommittedBucketFile() ;
      
        // Creates the shuttle launcher, will be launched by external task
        bucketShuttleLauncher = new BucketShuttleLauncher(this) ;
	}
	

	
	
	private File getLatestCommitedFile() throws NeedleManagerException {
        // Browse directory for needle files
		Long indexNumber = -1L ;
        String files[] = tableDir.list() ;
        if ( files == null )
        	return null ;
		for ( String file : files) {
			// Verifies file name pattern and extracts file number
			int extIndex = file.indexOf(EXTENSION) ;
			if ( extIndex <= 0 )
				continue ;
			long fileNumber = -1 ;
			String hexString = file.substring(0, extIndex) ;
			try {
				fileNumber = Long.parseLong(hexString, 16) ;
			} catch ( Throwable th ) {
				fileNumber = -1 ;
			}
			if ( fileNumber < 0 ) {
				continue ;
			}
			if ( fileNumber > indexNumber) {
				indexNumber = fileNumber ;
			}
		}
		if ( indexNumber < 0 )
			return null ;
			
		File indexFile = getFile(indexNumber,EXTENSION) ;
		if ( !indexFile.canRead() ) {
			throw new BucketTableManagerException("No read access to commited index file"+indexFile.getAbsolutePath()) ;
		}
	    return indexFile ;	
	}
	
	private File getNextFile(File latestCommittedFile) throws NeedleManagerException {
		long fileNumber = -1 ;
		if ( latestCommittedFile == null )
			fileNumber = 0L ;
		else {
	  		// Verifies file name pattern and extracts file number
			int extIndex = latestCommittedFile.getName().indexOf(EXTENSION) ;
			if ( extIndex <= 0 )
				throw new BucketTableManagerException("Latest committed file has a wrong name: "+latestCommittedFile.getName()) ;
			
			fileNumber = -1 ;
			String hexString = latestCommittedFile.getName().substring(0, extIndex) ;
			try {
				fileNumber = Long.parseLong(hexString, 16) ;
			} catch ( Throwable th ) {
				fileNumber = -1 ;
			}
			if ( fileNumber < 0 ) {
				throw new BucketTableManagerException("Latest committed file has a negative index number: "+latestCommittedFile.getName()) ;
			}
			fileNumber++; // Next!
		}
			
		File indexFile = getCreateFile(fileNumber,UEXTENSION) ;
		if ( !indexFile.canRead() ||! indexFile.canWrite() ) {
			throw new BucketTableManagerException("No read/write access to new index file"+indexFile.getAbsolutePath()) ;
		}
	    return indexFile ;	
	}
	
	private File getCommittedFile(File unCommittedFile) throws BucketTableManagerException {
  		// Verifies file name pattern and extracts file number
		int extIndex = unCommittedFile.getName().indexOf(UEXTENSION) ;
		if ( extIndex <= 0 )
			throw new BucketTableManagerException("Latest uncommitted file has a wrong name: "+unCommittedFile.getName()) ;
	
		String hexString = unCommittedFile.getName().substring(0, extIndex) ;
		
		return new File(tableDir, hexString+EXTENSION) ;
	}
	
	
	private File getFile(long indexNumber, String extension) throws BucketTableManagerException {
		String fileName = Long.toHexString(indexNumber) ;
		while ( fileName.length() < 16 )
			fileName = "0"+fileName ;
		fileName = fileName + extension ;
		File indexFile = new File(tableDir, fileName) ;
		return indexFile ;
	}
	
	private File getCreateFile(long indexNumber, String extension) throws BucketTableManagerException {
		File indexFile = null ;
		try {
		String fileName = Long.toHexString(indexNumber) ;
		while ( fileName.length() < 16 )
			fileName = "0"+fileName ;
		fileName = fileName + extension ;
		indexFile = new File(tableDir, fileName) ;
		if ( indexFile.exists() )
			indexFile.delete() ;
		indexFile.createNewFile() ;
		return indexFile ;
		} catch ( IOException ie ) {
			throw new BucketTableManagerException("Error creating bucket table file "+indexFile==null?"null":indexFile.getAbsolutePath()) ;
		}
	}
	
	

	
	private void initializeBucketTableFromLastCommittedBucketFile() throws BucketTableManagerException {
		FileInputStream tableStream = null ;
		FileChannel fileChannel = null;
		try {
			File latestCommittedFile = getLatestCommitedFile() ;
			if ( latestCommittedFile != null ) {
				tableStream = new FileInputStream(latestCommittedFile) ;
				fileChannel = tableStream.getChannel() ;
				ByteBuffer buffer = ByteBuffer.allocate(HEADERSIZE) ;
				fileChannel.position(0L) ; 
				int read = fileChannel.read(buffer) ;
				if ( read < HEADERSIZE ) {
					fileChannel.close() ;
					throw new BucketTableManagerException("Wrong bucket table header size: "+read+"/"+HEADERSIZE) ;
				}
				// Check content of header. Start with Big Endian (default for Java)
				buffer.rewind() ;
				byteOrder = ByteOrder.BIG_ENDIAN ;
				buffer.order(byteOrder) ;
				int magic = buffer.getInt() ;
				if ( magic == MAGICSTART_BADENDIAN ) {
					byteOrder = ByteOrder.LITTLE_ENDIAN ;
					buffer.order(byteOrder) ;
				} else if ( magic != MAGICSTART ) {
					fileChannel.close() ;
					throw new BucketTableManagerException("Bad header in bucket table file") ;
				}
				// Read number of buckets
				long headerMapSize = buffer.getLong() ;
				// Read checkPoint
				NeedlePointer includedCheckpoint = new NeedlePointer() ;
				includedCheckpoint.getNeedlePointerFromBuffer(buffer) ;
				// Read second magic number
				magic = buffer.getInt() ;
				if ( magic != MAGICEND ) {
					fileChannel.close() ;
					throw new BucketTableManagerException("Bad header in bucket table file") ;
				}
				// Check number of buckets against requested map size
				if ( headerMapSize != mapSize ) {
					// Map size does not match
					fileChannel.close() ;
					throw new BucketTableManagerException("Requested map size "+mapSize+" does not match header map size "+headerMapSize) ;
				}
				// Sets initial checkpoint
				bucketTable.setInitialCheckPoint(includedCheckpoint) ;
				// Now reads all entries
				logger.info("Hot start: loading buckets...") ;
				for ( int i = 0 ; i < nbBuffers ; i++ ) {
					bucketTable.prepareBufferForReading(i) ;
					read = fileChannel.read(bucketTable.getBuffer(i)) ;
					if ( read < bucketTable.getBuffer(i).limit() )
						throw new BucketTableManagerException("Incomplete bucket table file "+latestCommittedFile.getName()+", expected "+mapSize+HEADERSIZE) ;
					//else
					//	logger.info("Hot start: loaded "+(i+1)*entriesPerBuffer+" buckets"+((i<(nbBuffers-1))?"...":"")) ;
				}
				// Checks second magic marker
				buffer = ByteBuffer.allocate(NeedleLogInfo.INFOSIZE) ;
				buffer.rewind() ; buffer.limit(INTSIZE) ;
				if ( fileChannel.read(buffer) < INTSIZE )
					throw new BucketTableManagerException("Incomplete bucket table file, missing secong magic number "+latestCommittedFile.getName()) ;
				buffer.rewind() ;
				magic = buffer.getInt() ;
				if ( magic != MAGICSTART ) {
					fileChannel.close() ;
					throw new BucketTableManagerException("Bad header in bucket table file") ;
				}
				// Now reads clean counters
				while ( true ) {
					buffer.rewind() ;
					buffer.limit(NeedleLogInfo.INFOSIZE) ;
					read = fileChannel.read(buffer) ;
					if ( read > 0 && read < NeedleLogInfo.INFOSIZE )
						throw new BucketTableManagerException("Incomplete bucket table file, log info too short "+latestCommittedFile.getName()+", expected "+mapSize+HEADERSIZE) ;
					if ( read <= 0 )
						break ;
					else {
						NeedleLogInfo nli = new NeedleLogInfo(useAverage) ;
						buffer.rewind() ;
						nli.getNeedleLogInfo(buffer) ;
						logInfoPerLogNumber.put(new Integer(nli.getNeedleFileNumber()), nli) ;
					}
				}
				logger.info("Hot start: loaded "+(nbBuffers*entriesPerBuffer)+" buckets") ;
				
			} else {
				// Empty file
				bucketTable.setInitialCheckPoint(new NeedlePointer()) ;
				bucketTable.format() ;
			}
		} catch ( IOException ie ) {
			throw new BucketTableManagerException("Failed initializing bucket table", ie) ;
		} catch ( BufferUnderflowException bue ) {
			throw new BucketTableManagerException("Bucket table too short", bue) ;
		} finally {
			if ( fileChannel != null ) {
				try {
					fileChannel.close() ;
				} catch (IOException ex) {
					throw new BucketTableManagerException("Error while closing file channel", ex) ;
				}
			}
		}
	}
	
	private void commitBucketTableToDisk() throws BucketTableManagerException {
		File currentFile = null ;
		FileChannel fileChannel = null;
		ByteBuffer headerBuffer = null;
		try {
			logger.warn("Start commit bucket table...") ;
			if ( bucketTable.getRequestedCheckPoint() == null ||
					bucketTable.getRequestedCheckPoint().isEmpty() )
				throw new BucketTableManagerException("commit requested while there is no requested checkpoint") ;
			currentFile = getLatestCommitedFile() ;
			File nextFile = getNextFile(getLatestCommitedFile()) ;
			fileChannel = (new RandomAccessFile(nextFile, "rw")).getChannel() ;
			// Write header with empty checkpoint 
			headerBuffer = ByteBuffer.allocate(HEADERSIZE) ;
			fileChannel.position(0L) ; 
			headerBuffer.putInt(MAGICSTART) ; 
			headerBuffer.putLong(mapSize) ;
			// NeedlePointer lastCheckPoint = bucketTable.getLastCheckPoint() ; // Reset checkpoint to no checkpoint done
			NeedlePointer lastCheckPoint = new NeedlePointer() ; // Empty needle
			lastCheckPoint.putNeedlePointerToBuffer(headerBuffer) ;
			headerBuffer.putInt(MAGICEND) ;
			headerBuffer.flip() ; // truncate buffer
			fileChannel.write(headerBuffer) ;
			// Now writes buffers
			for ( int i = 0 ; i < nbBuffers ; i++ ) {
				bucketTable.prepareBufferForWriting(i) ;
				int written = fileChannel.write(bucketTable.getBuffer(i)) ;
				if ( written < bucketTable.getBuffer(i).limit() )
					throw new BucketTableManagerException("Incomplete write for bucket table file "+nextFile.getName()+", expected "+mapSize+HEADERSIZE) ;
				// else
					// logger.info("Bucket table commit: written "+(i+1)*entriesPerBuffer+" buckets"+((i<(nbBuffers-1))?"...":"")) ;
				try {
					Thread.sleep(10) ;
				} catch ( Throwable th ) {
					
				}
			}
			// Writes second magic number
			ByteBuffer buffer = ByteBuffer.allocate(NeedleLogInfo.INFOSIZE) ;
			buffer.rewind() ; buffer.limit(INTSIZE) ;
			buffer.putInt(MAGICSTART) ;
			buffer.rewind() ;
			fileChannel.write(buffer) ;
			// Write Needle Log Info
			Iterator <NeedleLogInfo> it = logInfoPerLogNumber.values().iterator() ;
			while ( it.hasNext() ) {
				buffer.rewind() ;
				buffer.limit(NeedleLogInfo.INFOSIZE) ;
				NeedleLogInfo nli = it.next() ;
				nli.putNeedleLogInfo(buffer, true) ;
				int written = fileChannel.write(buffer) ;
				if ( written < NeedleLogInfo.INFOSIZE )
					throw new BucketTableManagerException("Incomplete write for bucket table file, writing log infos "+nextFile.getName()) ;
			}
			// Writes checkpoint
			headerBuffer = ByteBuffer.allocate(NeedlePointer.POINTERSIZE) ;
			headerBuffer.rewind() ;
			headerBuffer.limit(NeedlePointer.POINTERSIZE) ;
			// System.out.println("Writing checkpoint in index "+bucketTable.getRequestedCheckPoint()) ;
			bucketTable.getRequestedCheckPoint().putNeedlePointerToBuffer(headerBuffer, true) ; // Flip buffer after write
			headerBuffer.rewind() ;
			// fileChannel.force(false) ;
			if ( fileChannel.write(headerBuffer, CHECKPOINTOFFSET) < NeedlePointer.POINTERSIZE ) {
				throw new BucketTableManagerException("Could not write checkpoint to "+nextFile.getName()) ;
			}
			fileChannel.force(true) ;
			fileChannel.close() ;
			if ( !nextFile.renameTo(getCommittedFile(nextFile)) )
				throw new BucketTableManagerException("Could not rename "+nextFile.getName()+" to "+getCommittedFile(nextFile).getName()) ;
			
			logger.warn("Committed bucket table.") ;
		} catch ( IOException ie ) {
			throw new BucketTableManagerException("Failed writting bucket table", ie) ;
		} finally {
			headerBuffer = null; //May ease garbage collection
			if ( fileChannel != null) {
				try {
					fileChannel.close();
				} catch(Exception ex) {
					throw new BucketTableManagerException("Failed to close file channel",ex);
				}
			}
		}
		try {
			if ( currentFile != null ) {
				if ( !currentFile.delete() )
					logger.error("Failed deleting previous bucket table"+currentFile.getName()) ;
			}
		} catch ( Throwable th ) {
			logger.error("Failed deleting previous bucket table"+currentFile.getName(), th) ;
		}
	}
	

	
	public NeedlePointer getNeedlePointer(long bucket) throws BucketTableManagerException {
		NeedlePointer needlePointer = bucketTable.get(bucket) ;
		if ( needlePointer == null )
			return needlePointer ;
		else if ( needlePointer.isEmpty() )
			return null ; // No need to look at disk: we know it's empty
		else
			return needlePointer ;
	}
	
	
	public void writeNeedlePointer(long bucket, NeedlePointer needlePointer) throws BucketTableManagerException { 
		writeNeedlePointer(bucket, needlePointer, false) ;
	}

	public void writeNeedlePointer(long bucket, NeedlePointer needlePointer, boolean single) throws BucketTableManagerException { 
		bucketTable.put(bucket, needlePointer) ;
	}
	

	/**
	 * @param full If true commits the full map, overwise make it smooth in multiple passes (Not implemented yet)
	 * @throws BucketTableManagerException
	 */
	public void forceCommit(NeedlePointer checkPoint) throws BucketTableManagerException {
		if ( checkPoint == null || checkPoint.isEmpty() )
			throw new BucketTableManagerException("Force commit with no checkpoint") ;
		bucketTable.requestCheckPoint(checkPoint) ;
		commitBucketTableToDisk() ;
		bucketTable.signalsCheckPointDoneAndCleanDirtyEntries() ;
	}
	

	
	public int getDirtyBuckets() {
		return bucketTable==null?0:bucketTable.getDirtyBuckets() ;
				
	}
	
	

	public NeedlePointer getCheckPoint() {
			return bucketTable.getLastCheckPoint();
	}


	
	public ByteOrder getByteOrder() {
		return byteOrder;
	}


	public long getMapSize() {
		return mapSize;
	}



	public int getPeriod() {
		return period;
	}
	
	/*
	
	public long getMaxCycleTimePass1() {
		return bucketShuttle==null?-1L:bucketShuttle.getMaxCycleTimePass1() ;
	}

	public long getMaxCycleTimePass2() {
		return bucketShuttle==null?-1L:bucketShuttle.getMaxCycleTimePass2() ;
	}
	
	public void resetMaxCycleTimes() {
		if ( bucketShuttle!=null )
			bucketShuttle.resetMaxCycleTimes() ;
	}
	*/
	
	
	
	public void setCycleTimePass1(long cycleTime) {
		synchronized (cycleTimeLock) {
			if ( this.maxCycleTimePass1 < cycleTime)
				this.maxCycleTimePass1 = cycleTime ;
		}
	}
	
	public void setCycleTimePass2(long cycleTime) {
		synchronized (cycleTimeLock) {
			if ( this.maxCycleTimePass2 < cycleTime)
				this.maxCycleTimePass2 = cycleTime ;
		}
	}
	
	public long getMaxCycleTimePass1() {
		synchronized (cycleTimeLock) {
			return maxCycleTimePass1;
		}
	}

	public long getMaxCycleTimePass2() {
		synchronized (cycleTimeLock) {
			return maxCycleTimePass2;
		}
	}
	
	public void resetMaxCycleTimes() {
		synchronized (cycleTimeLock) {
			this.maxCycleTimePass1 = -1L;
			this.maxCycleTimePass2 = -1L ;
		}
	}
	
	public void waitSync() {
		synchronized ( cyclingLock ) {
			
		}
	}
	

	public BucketFNVHash getBucketFNVHash() {
		return bucketFNVHash;
	}
	
	public long hash(byte[] key) {
		return bucketFNVHash==null?-1L:bucketFNVHash.hash(key);
	}

	public long hash(String key) {
		return bucketFNVHash==null?-1L:bucketFNVHash.hash(key);
	}
	
	/*
	public void waitSync() {
		if ( bucketShuttle!= null ) 
			bucketShuttle.waitSync() ;
	}
	*/

	
	
	private static final long TEST_COUNT = 5000000L ;
	private static final long TARGET_TPS = 8000L ;

	public static void main(String[]args){
		
		try {
			BucketTableManager nw = new BucketTableManager("/Users/Francois/Documents/NEEDLES/0.TestStore", 100, 1000000, 3000L, true) ;
			
			nw.initialize() ;
			System.out.println("Bucket table initialization: "+(nw.getCheckPoint().isEmpty()?" Repair required":" OK")) ;
			
			Runtime runtime = Runtime.getRuntime();
			long before = runtime.totalMemory() - runtime.freeMemory() ;
			
		
			nw.getCheckPoint().copyFrom(new NeedlePointer()) ; // reset checkpoint
			
			Date startDate = new Date() ;
			Date lapDate = new Date() ;
			System.out.println("Start test for "+TEST_COUNT+" pointers") ;
			long counter = 0 ;
			long lapCounter = 0 ;
			long offset = 0 ;
			BucketFNVHash hashFunc = new BucketFNVHash(1000000000L) ;
			while ( counter < TEST_COUNT ) {
				NeedlePointer needlePointer = new NeedlePointer() ;
				String key = "MYVERYNICEKEY"+counter ;
				long bucket = hashFunc.hash(key) ;
				needlePointer.setNeedleFileNumber(4) ;
				needlePointer.setNeedleOffset(offset++) ;
				nw.writeNeedlePointer(bucket, needlePointer) ;
				counter++ ; 
				Date lapDate2 = new Date() ;
				long spent = lapDate2.getTime()-lapDate.getTime() ;
				if ( spent >= 1000 || (counter - lapCounter > TARGET_TPS) ) { // Check each second or target tps
					if ( spent < 1000 ) { // pause when tps reached
						Thread.sleep(1000-spent) ;
						System.out.print(".") ;
					} else
						System.out.print("*") ;
					lapDate = lapDate2 ; // Reset lap time
					lapCounter = counter ; // Reset tps copunter
				}		
			}
			long timeSpent = new Date().getTime() - startDate.getTime() ;
			System.out.println("\n\nWriting before cache commit of "+TEST_COUNT+" pointers \n"+
						"\tTotal time: "+timeSpent/1000+"s\n"+
						"\tThroughput: "+(TEST_COUNT/timeSpent*1000)+" tps") ;
		
			timeSpent = new Date().getTime() - startDate.getTime() ;
			System.out.println("\n\nProcessed writing of "+TEST_COUNT+" pointers \n"+
						"\tTotal time: "+timeSpent/1000+"s\n"+
						"\tThroughput: "+(TEST_COUNT/timeSpent*1000)+" tps") ;
			System.out.println("Max cycle time = "+ (nw.getMaxCycleTimePass1()+nw.getMaxCycleTimePass2())) ;
			
			
		} catch ( Throwable th ) {
			th.printStackTrace() ;
		}
		
	}



	public long getSyncPeriod() {
		return syncPeriod;
	}



	public void setSyncPeriod(int syncPeriod) {
		this.syncPeriod = syncPeriod;
	}



	public BucketShuttleLauncher getBucketShuttleLauncher() {
		return bucketShuttleLauncher;
	}


	public void moveNeedle(int originalFileNumber, int originalSize, int newFileNumber, int newSize) {
		// 1) gets previous block info and removes one needle
		if ( originalFileNumber >= 0 ) {
			NeedleLogInfo oldNli = logInfoPerLogNumber.get(new Integer(originalFileNumber)) ;
			if ( oldNli != null ) {
				oldNli.addDeadNeedle(originalSize) ;
			}
		}
		// 2) gets new block info , and create info if required
		NeedleLogInfo freshNli = new NeedleLogInfo(useAverage) ;
		freshNli.setNeedleFileNumber(newFileNumber) ; 
		NeedleLogInfo newNli = logInfoPerLogNumber.putIfAbsent(new Integer(newFileNumber), freshNli) ;
		// 3) add one needle in the info
		if ( newNli != null )
			newNli.addLiveNeedle(new Date().getTime(), newSize) ;
		else
			freshNli.addLiveNeedle(new Date().getTime(), newSize) ;
	}
	
	public void removeLogInfo(int needleFileNumber) {
		if ( needleFileNumber < 0 ) {
			return ;
		}
		logInfoPerLogNumber.remove(new Integer(needleFileNumber)) ;
	}
	

	public ArrayList<NeedleLogInfo> getSortedLogInfo() throws BucketTableManagerException {
		ArrayList<NeedleLogInfo> result = new ArrayList<NeedleLogInfo>() ;
		if ( logInfoPerLogNumber.isEmpty() )
			return result ; // No more work: no log info file
		Iterator <NeedleLogInfo>it = logInfoPerLogNumber.values().iterator() ;
		while ( it.hasNext() ) {
			NeedleLogInfo nli = it.next().clone() ;
			result.add(nli) ;
		}
		Collections.sort(result) ;
		return result ;	
	}
	
	public String getKeyDirPath() {
	    return keyDirPath;
	}
	
	
}
