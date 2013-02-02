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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;

import voldemort.versioning.VectorClock;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.yobidrive.diskmap.Chrono;
import com.yobidrive.diskmap.buckets.BucketTableManagerException;




/**
 * Created by EZC Group S.A.
 * @author Francois Vaille
 * 
 * Append only needle log files (Deletions are processed by creation of a new needle with same key and delete flag
 *
 */
public class NeedleManager {
	private final int MAXKEYSIZE = 1024 ;
	private final int MAXVERSIONSIZE = 2048 ;
	private final int MAXDATASIZE = 1024 * 512 ;
	private final String EXTENSION = ".ydm" ; // YobiDrive Disk Map Logs
	// private final String IMPORT_EXTENSION = ".ydr" ; // YobiDrive Disk Map Rebuild Logs
	
    long logSize ; 				// Size of one log file. After reaching this threshold a new file is created
    String logPath ; 			// Data Files directory path
    String mode ;				// File write mode
    private int logNumber = -1 ;// Current write log file
    private File logDir ;
    private File importDir ;
    private long maxCachedBytes = 1000000L ;
    private long maxCachedHeaderBytes = 100000L ;
    int readThreads = 8 ;
    
    private static Log logger = LogFactory.getLog(NeedleManager.class);
    private ConcurrentHashMap<Integer, FileChannel> channelMap = new ConcurrentHashMap<Integer, FileChannel>() ;
    private ByteBuffer needleBuffer = ByteBuffer.allocateDirect(MAXKEYSIZE+MAXVERSIONSIZE+MAXDATASIZE+Needle.NEEDLEOVERHEAD) ;
    private BlockingQueue<ByteBuffer> threadBufferQ = null ;
    
    private LoadingCache<NeedlePointer, Needle> needleReadCache = null ;

    private LoadingCache<NeedlePointer, NeedleHeader> needleHeaderReadCache = null ;
    
    
    
    private NeedlePointer repairNeedle = new NeedlePointer() ;
    private NeedlePointer compactNeedle = new NeedlePointer() ;
    private NeedlePointer importNeedle = new NeedlePointer() ;
    private FileChannel importChannel = null ;
    // private NeedlePointer cleanNeedle = new NeedlePointer() ;
    FileChannel writeChannel = null ; // Channel currently writing
    long writePosition = 0L ; 		  // Position for writing in channel between prepareWrite & commitWrite
    private ByteBuffer compactBuffer = ByteBuffer.allocateDirect(MAXKEYSIZE+MAXVERSIONSIZE+MAXDATASIZE+Needle.NEEDLEOVERHEAD) ;
    private ByteBuffer importBuffer = ByteBuffer.allocateDirect(MAXKEYSIZE+MAXVERSIONSIZE+MAXDATASIZE+Needle.NEEDLEOVERHEAD) ;
    private int cachePreload = 0 ;
    
    
    
	public NeedleManager(String logPath, long logSize, int readThreads, long maxCachedHeaderBytes, long maxCachedBytes, int cachePreload) throws NeedleManagerException {
        this.logPath = logPath;
        this.logSize = logSize ;
        this.mode = "rw" ;
        this.maxCachedBytes = maxCachedBytes ;
        this.maxCachedHeaderBytes = maxCachedHeaderBytes ;
        this.readThreads = readThreads ;
        this.cachePreload = cachePreload ;
    }
	
	public boolean isRebuildRequired() throws NeedleManagerException {
        // Checks existence of rebuild-required-file
        File rebuildRequiredFile = new File(logDir, "rebuild-required.ydc") ;
        boolean result = rebuildRequiredFile.exists() ;
        if ( result ) {
        	// Check if we will be able to remove it after processing
        	if ( !rebuildRequiredFile.canWrite() ) {
        		logger.error("Can't write "+rebuildRequiredFile.getAbsolutePath()) ;
            	throw new NeedleManagerException() ;
        	}
        }
        return result ;
	}
	
	public void signalRebuildInProgress() throws NeedleManagerException {
        // Checks existence of rebuild-required-file
        File rebuildRequiredFile = new File(logDir, "rebuild-required.ydc") ;
    
        if ( rebuildRequiredFile.exists() ) {
        	// Remove it
        	rebuildRequiredFile.delete() ;
        }
	}
	
	public void initialize() throws NeedleManagerException {
		// Gets latest needle file
        logDir = new File(logPath) ;
        if ( !logDir.exists() ) {
        	logDir.mkdir() ;
        }
        if ( !logDir.exists() || !logDir.isDirectory() ) {
        	logger.error("Wrong directory "+logPath) ;
        	throw new NeedleManagerException() ;
        }
        importDir = new File(logDir,"import") ;
	if ( !importDir.exists() ) {
	    importDir.mkdir() ;
	}
	if ( !importDir.exists() || !importDir.isDirectory() ) {
    		logger.error("Wrong directory "+importDir.getAbsolutePath()) ;
    		throw new NeedleManagerException() ;
	}
        // Checks if rebuild required
        if ( isRebuildRequired() ) {
        	// Moves the files as .ydr for later rebuild (taken as bulk input)
        	renameNeedleFilesForRebuild() ;
        	signalRebuildInProgress() ;
        } else
        	initializeNeedleFiles() ;
        // Initialize empty first needle if required, reader buffers and cache
        initializeBuffersAndCache() ;
	}
    
    private void initializeNeedleFiles() throws NeedleManagerException {
        // Browse directory for needle files (won't find any if rebuild required)
        String files[] = logDir.list() ;
        if ( files != null ) {
			for ( String file : files) {
				// Verifies file name pattern and extracts file number
				int extIndex = file.indexOf(EXTENSION) ;
				if ( extIndex <= 0 )
					continue ;
				int fileNumber = -1 ;
				String hexString = file.substring(0, extIndex) ;
				try {
					fileNumber = Integer.parseInt(hexString, 16) ;
				} catch ( Throwable th ) {
					fileNumber = -1 ;
				}
				if ( fileNumber < 0 ) {
					// Normal situation: non log files (including the bucket file)
					continue ;
				}
				if ( fileNumber > logNumber)
					logNumber = fileNumber ;
				File needleFile = new File(logDir,file) ;
				if ( !needleFile.canRead() || !needleFile.canWrite() ) {
					logger.error("No read/write access to "+logPath+"/"+file) ;
		        	throw new NeedleManagerException() ;
				}
				RandomAccessFile needleRFile ;
				try {
					needleRFile = new RandomAccessFile(needleFile, mode) ; // AutoCommit of content writes (rws), content + meta (rwd), or nothing (rw)
				} catch ( Throwable th ) {
					logger.error("File not found "+logPath+"/"+file) ;
		        	throw new NeedleManagerException() ;
				}
				FileChannel needleChannel = needleRFile.getChannel() ;
				channelMap.putIfAbsent(fileNumber, needleChannel) ;
			}
        }   
    }
    
    
    private void initializeBuffersAndCache() throws NeedleManagerException {
		if ( logNumber < 0 )
			createNewLog() ;
		// Now prepare read threads
		threadBufferQ = new ArrayBlockingQueue<ByteBuffer>(readThreads, true) ;
        try {
	        for ( int i = 0 ; i < readThreads ; i++ ) {
	        	ByteBuffer needleBuffer = ByteBuffer.allocateDirect(MAXKEYSIZE+MAXVERSIONSIZE+MAXDATASIZE+Needle.NEEDLEOVERHEAD) ;
	        	threadBufferQ.put(needleBuffer) ;
	        }
        } catch ( Throwable th ) {
        	logger.error("Error building needle reader buffers", th) ;
        }
        // Finally create readCaches
        NeedleWeighter needleWeighter = new NeedleWeighter() ;
        needleReadCache 		= CacheBuilder.newBuilder().weigher(needleWeighter).maximumWeight(this.maxCachedBytes).build(new NeedleCacheLoader(this)) ;
        NeedleHeaderWeighter needleHeaderWeighter = new NeedleHeaderWeighter() ;
        needleHeaderReadCache 	= (CacheBuilder.newBuilder()).weigher(needleHeaderWeighter).maximumWeight(this.maxCachedHeaderBytes).build(new NeedleHeaderCacheLoader(this)) ;
        logger.info(needleHeaderReadCache.stats().toString()) ;
        // Create compacting buffer
        compactBuffer = ByteBuffer.allocateDirect(MAXKEYSIZE+MAXVERSIONSIZE+MAXDATASIZE+Needle.NEEDLEOVERHEAD) ;
	}
	
	
	public void renameNeedleFilesForRebuild() throws NeedleManagerException {
        // Browse directory for needle files
        String files[] = logDir.list() ;
        if ( files == null )
        	return ;
		for ( String file : files) {
			// Verifies file name pattern and extracts file number
			int extIndex = file.indexOf(EXTENSION) ;
			if ( extIndex <= 0 )
				continue ;
			
			String hexString = file.substring(0, extIndex) ;
			
			File needleFile = new File(logDir,file) ;
			// create import subdirectory if not exist
			
			
			File rebuildNeedleFile = new File(importDir,hexString+EXTENSION) ;
			if ( !needleFile.renameTo(rebuildNeedleFile) ) {
				logger.error("Failed renaming "+needleFile.getAbsolutePath()+" to "+rebuildNeedleFile.getAbsolutePath()) ;
	        	throw new NeedleManagerException() ;
			}
		}
	}
	
	public void removeLogFile(int logNumber) throws NeedleManagerException {
		closeChannel(logNumber) ; // Close channel
		File fileToDelete = getFile(logNumber) ;
		if ( !fileToDelete.delete() )
			throw new NeedleManagerException("Could not delete "+fileToDelete.getName()+" after cleaning") ;
	}
	
	public void removeImportFile(int logNumber) throws NeedleManagerException {
		File fileToDelete = getImportFile(logNumber) ;
		if ( !fileToDelete.delete() )
			throw new NeedleManagerException("Could not delete "+fileToDelete.getName()+" after import") ;
	}
	
	public void stopService() {
		// Close service
        syncAll() ;
	}
	
	
	private FileChannel createNewLog() throws NeedleManagerException {
		logNumber++ ;
		File needleFile = getFile(logNumber) ;
		boolean okCreate = false ;
		
		try { 
			if ( needleFile.exists()) {
				okCreate = true ;
			} else {
				okCreate = needleFile.createNewFile() ; 
			}
		} catch ( IOException e ) { okCreate = false ; }
		if ( !okCreate && !needleFile.exists() ) {
			logger.error("File "+logPath+"/"+needleFile.getName()+" already exists, must have been manually added since starting engine!") ;
        	throw new NeedleManagerException() ;
		}
		
		return getChannel(logNumber) ;
	}
	
	private File getFile(int logNumber) throws NeedleManagerException {
		String fileName = Integer.toHexString(logNumber) ;
		while ( fileName.length() < 8 )
			fileName = "0"+fileName ;
		fileName = fileName + EXTENSION ;
		File needleFile = new File(logDir, fileName) ;
		return needleFile ;
	}
	
	private File getImportFile(int logNumber) throws NeedleManagerException {
		String fileName = Integer.toHexString(logNumber) ;
		while ( fileName.length() < 8 )
			fileName = "0"+fileName ;
		fileName = fileName + EXTENSION ;
		File needleFile = new File(importDir, fileName) ;
		return needleFile ;
	}
	

	
	/** Commits pending needle write by replacing the needle pointer in buffer and writing to disk at prepared position
	 * ONLY the needle pointer should be changed since prepareWrite, other changes will be lost
	 * @param needlePointer
	 * @return
	 * @throws NeedleManagerException
	 */
	public void commitWrite(Needle needle) throws NeedleManagerException {
		// Rewinds write buffer and fills with needle
		try {
			// Fill needle buffer
			needleBuffer.rewind() ;
			needle.putNeedleInBuffer(needleBuffer) ;
			writeChannel.position(writePosition) ;
			
			while ( needleBuffer.hasRemaining() ) {
				writeChannel.write(needleBuffer);
			}
			
//			int written = writeChannel.write(needleBuffer) ;
//			if ( written < needleBuffer.limit() ) {
//				logger.error("Error writing needle "+needle.getKey()+", not all bytes written") ;
//				throw new NeedleManagerException()	;
//			}
			
			// Manages pre-caching of written needle
			if ( cachePreload > 0 ) {
			    NeedlePointer needlePointer = new NeedlePointer() ;
			    needlePointer.setNeedleFileNumber(logNumber) ;
			    needlePointer.setNeedleOffset(writePosition) ;
			    needleReadCache.put(needlePointer, needle) ;
			    if ( cachePreload == 2 )
				needleHeaderReadCache.put(needlePointer, needle.getNeedleHeader(needlePointer)) ;
			}
		} catch ( Throwable th ) {
			logger.error("Error writing needle "+needle.getKey(), th) ;
			throw new NeedleManagerException()	;
		}
	}
	
	/** Gets disk file/offset, prepares buffer
	 * @param needle
	 * @return the future position to be committed by write
	 * @throws NeedleManagerException
	 */
	public NeedlePointer  prepareWriteNeedle(Needle needle) throws NeedleManagerException {
		// Rewinds write buffer and fills with needle
		try {
			writeChannel = null ;
			// needleBuffer.rewind() ;
			// needle.putNeedleInBuffer(needleBuffer) ;
			// Set file write position to the end
			writeChannel = channelMap.get(logNumber) ;
			// FileChannel acquired: checks if enough space in file or missing file
			if ( writeChannel == null || writeChannel.size() + needle.getWriteTotalSize() > logSize ) {
				// File has been deleted: move forward
				writeChannel = createNewLog() ;
			}
			if ( writeChannel == null ) {
				logger.error("Error writing needle "+needle.getKey()+", no channel open") ;
				throw new NeedleManagerException()	;
			}
			// FileChannel acquired: position and write
			writePosition = writeChannel.size() ;
			
			NeedlePointer needlePointer = new NeedlePointer() ;
			needlePointer.setNeedleFileNumber(logNumber) ;
			needlePointer.setNeedleOffset(writePosition) ;
			return needlePointer ;
		} catch ( Throwable th ) {
			logger.error("Error writing needle "+needle.getKey(), th) ;
			throw new NeedleManagerException()	;
		}
	}
	
	
	
	public void close() {
		Enumeration <FileChannel> channels = channelMap.elements() ;
		while ( channels.hasMoreElements() ) {
			FileChannel fc = channels.nextElement() ;
			try {
				fc.force(true) ;
				fc.close() ;
			} catch ( Throwable th ) {
				logger.error("Error closing needle channel", th) ;
			}
		}
	}
	
	public void syncAll() {
		Enumeration <FileChannel> channels = channelMap.elements() ;
		while ( channels.hasMoreElements() ) {
			FileChannel fc = channels.nextElement() ;
			try {
				fc.force(true) ;
			} catch ( Throwable th ) {
				logger.error("Error synching needle channel", th) ;
			}
		}
		// logger.info(needleHeaderReadCache.stats().toString()) ;
	}
	
	private FileChannel getChannel(int logNumber) throws NeedleManagerException {
		FileChannel fc = channelMap.get(new Integer(logNumber)) ;
		if ( fc == null ) {
			// File Channel not created yet: create and keep
			String fileName = Integer.toHexString(logNumber) ;
			while ( fileName.length() < 8 )
				fileName = "0"+fileName ;
			fileName = fileName + EXTENSION ;
			File needleFile = new File(logDir, fileName) ;
			if ( needleFile.exists() && needleFile.canRead() ) {
				RandomAccessFile needleRFile ;
				try {
					needleRFile = new RandomAccessFile(needleFile, mode) ; // AutoCommit of content writes (rws), content + meta (rwd), or nothing (rw)
				} catch ( Throwable th ) {
					logger.error("Needle log file not found "+logPath+"/"+fileName) ;
		        	throw new NeedleManagerException() ;
				}
				FileChannel needleChannel = needleRFile.getChannel() ;
				channelMap.putIfAbsent(logNumber, needleChannel) ;
			}
			return channelMap.get(new Integer(logNumber)) ;
		} else
			return fc ;
		
	}
	
	
	public FileChannel getImportChannel(int logNumber) throws NeedleManagerException {
		return getImportChannel(logNumber,EXTENSION, false);
	}
	
	/**
	 * USed for recovery, same as the import channel but with the ydm extension
	 * @param logNumber
	 * @return
	 * @throws NeedleManagerException
	 */
	public FileChannel getRecoveryChannel(int logNumber) throws NeedleManagerException {
		return getImportChannel(logNumber,EXTENSION, true);
	}
	
	private FileChannel getImportChannel(int logNumber, String extension, boolean recovery) throws NeedleManagerException {
		FileChannel needleChannel = null ;
		String fileName = Integer.toHexString(logNumber) ;
		while ( fileName.length() < 8 )
			fileName = "0"+fileName ;
		fileName = fileName + extension ;
		File needleFile = new File(recovery?logDir:importDir, fileName) ;
		if ( needleFile.exists() && needleFile.canRead() ) {
			RandomAccessFile needleRFile ;
			try {
				needleRFile = new RandomAccessFile(needleFile, "r") ; // Read only mode
			} catch ( Throwable th ) {
				logger.error("Needle log file not found "+logPath+"/"+fileName) ;
	        	throw new NeedleManagerException() ;
			}
			needleChannel = needleRFile.getChannel() ;
		}
		return needleChannel ;
	}
	
	public int getLastFileNumber() {
		int result = -1 ;
		Set<Integer> channelSet = channelMap.keySet() ;
		if ( channelSet == null )
			return result ;
		Iterator<Integer> it = channelSet.iterator() ;
		while ( it.hasNext() ) {
			int channelFileNumber = it.next().intValue() ;
			if ( channelFileNumber > result )
				result = channelFileNumber ;
		}
		
		return result ;
	}
	
	public int getFirstFileNumber() {
		int result = Integer.MAX_VALUE ;
		Set<Integer> channelSet = channelMap.keySet() ;
		if ( channelSet == null )
			return -1 ;
		Iterator<Integer> it = channelSet.iterator() ;
		while ( it.hasNext() ) {
			int channelFileNumber = it.next().intValue() ;
			if ( channelFileNumber < result )
				result = channelFileNumber ;
		}
		
		return result==Integer.MAX_VALUE?-1:result ;
	}
	
	public boolean checkChannel(int logNumber) throws NeedleManagerException {
		return channelMap.containsKey(logNumber) ;
	}
	
	private void closeChannel(int logNumber) throws NeedleManagerException {
		FileChannel fc = channelMap.get(logNumber) ;
		if ( fc == null )
			return ;
		try {
			fc.close() ;
		} catch ( IOException ie ) {
			logger.error("Error closing log channel "+Integer.toHexString(logNumber)) ;
			throw new NeedleManagerException("Error closing log channel "+Integer.toHexString(logNumber), ie) ;
		}
		channelMap.remove(new Integer(logNumber)) ;
	}
	
	/** Loads the first needle matching the key and returns
	 * @param initialNeedlePointer
	 * @param key
	 * @return
	 */
	public Needle readNextNeedleByKey(NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
		if ( key == null || key.length == 0 )
			return null ; // Stupid developer proof...
		
		Needle needle = getNeedleFromCache(initialNeedlePointer) ;
		while ( needle != null && !needle.isNull() ) {
			// Check key
			if ( Arrays.equals(key, needle.getKeyBytes()) )
				return needle ; // This needle is the right one
			// Gets next needle
			NeedlePointer currentPointer = needle.getPreviousNeedle() ;
			if ( currentPointer == null )
				return null ; // No more needles
			needle = getNeedleFromCache(currentPointer.clone()) ; // Fetch next needle
		}
		return null ; // No needles found	
	}
	
	/** Loads the needle chain from the bucket up to the needle with the desired key
	 * @param initialNeedlePointer
	 * @param key
	 * @return The chain of needles. Target needle is the last in the chain.
	 */
	public ArrayList<Needle> getNeedleChain(NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
		ArrayList<Needle> result = new ArrayList<Needle>() ;
		
		if ( key == null || key.length == 0 )
			return null ; // Stupid developer proof...
		
		Needle needle = getNeedleFromCache(initialNeedlePointer) ;
		while ( needle != null && !needle.isNull() ) {
			// Add needle to the chain
			result.add(needle) ;
			// Check key
			if ( Arrays.equals(key, needle.getKeyBytes()) )
				return result ; // This needle is the right one: stop there
			// Gets next needle
			NeedlePointer currentPointer = needle.getPreviousNeedle() ;
			if ( currentPointer == null )
				return null ; // No more needles: we haven't found the requested key
			needle = getNeedleFromCache(currentPointer.clone()) ; // Fetch next needle
		}
		return null ; // No more needles: we haven't found the requested key
	}
	
	/** Loads the needle header chain from the bucket up to the needle with the desired key
	 * @param initialNeedlePointer
	 * @param key
	 * @return The chain of needles. Target needle is the last in the chain.
	 */
	public ArrayList<NeedleHeader> getNeedleHeaderChain(NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
		ArrayList<NeedleHeader> result = new ArrayList<NeedleHeader>() ;
		
		if ( key == null || key.length == 0 )
			return null ; // Stupid developer proof...
		
		NeedleHeader needleHeader = getNeedleHeaderFromCache(initialNeedlePointer) ;
		while ( needleHeader != null && !needleHeader.isNull() ) {
			// Add needle to the chain
			result.add(needleHeader) ;
			// Check key
			if ( Arrays.equals(key, needleHeader.getKeyBytes()) )
				return result ; // This needle is the right one: stop there
			// Gets next needle
			NeedlePointer currentPointer = needleHeader.getPreviousNeedle() ;
			if ( currentPointer == null )
				return null ; // No more needles: we haven't found the requested key
			needleHeader = getNeedleHeaderFromCache(currentPointer.clone()) ; // Fetch next needle
		}
		return null ; // No more needles: we haven't found the requested key
	}
	
	/** Feeds the full needle header chain linked to the bucket and returns the index of the desired key
	 * Eliminates duplicates: for each key the first one if the latest one
	 * @param needleHeaderChain An empty chain to feed
	 * @param initialNeedlePointer
	 * @param key
	 * @return the index of the desired key in the chain / -1 if not found
	 */
	public int feedNeedleHeaderChain(ArrayList<NeedleHeader> needleHeaderChain, NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
		int result = -1 ;
		int counter = 0 ;
		ArrayList <VectorClock> versionList = new ArrayList <VectorClock>() ;
		ArrayList <Boolean> deletedList = new ArrayList <Boolean>() ;
		int latestVersionIndex = -1 ;
		VectorClock latestVectorClock = null ;
		if ( key == null || key.length == 0 )
			return -1 ; // Stupid developer proof...
		
		NeedleHeader needleHeader = getNeedleHeaderFromCache(initialNeedlePointer) ;
		while ( needleHeader != null && !needleHeader.isNull() ) {
			// Add needle to the chain if not already in
			if ( !isNeedleHeaderInChain(needleHeaderChain, needleHeader.getKeyBytes()))
				needleHeaderChain.add(needleHeader) ;
			// Check key
			if ( Arrays.equals(key, needleHeader.getKeyBytes()) ) {
			    	versionList.add(needleHeader.getVersion()) ;
			    	/*if ( latestVectorClock == null ) {
			    	    latestVectorClock = needleHeader.getVersion().clone() ;
			    	    latestVersionIndex = versionList.size() -1 ;
			    	} else {
			    	    
			    	}*/
			    	deletedList.add(new Boolean(needleHeader.isDeleted())) ;
				result = needleHeaderChain.size()-1 ;
				counter++ ;
			}
			// Gets next needle
			NeedlePointer currentPointer = needleHeader.getPreviousNeedle() ;
			if ( currentPointer == null )
				break ;
			needleHeader = getNeedleHeaderFromCache(currentPointer.clone()) ; // Fetch next needle
		}
		if ( counter > 1 ) {
		    String vList = "\n" ;
		    for ( int i=0; i < versionList.size(); i++) {
			vList += versionList.get(i).toString() +"("+(deletedList.get(i)?"DEL":"STD")+"\n" ;
		    }
		    logger.error(counter+" needles in chain for key "+getKeyAsString(key)+", List="+vList+'('+')') ;
		    
		    
		}
		return result ; // No more needles: we haven't found the requested key
	}
	
	private String getKeyAsString(byte[] key) {
	    try {
		return new String(key,"UTF-8") ;
	    } catch ( Throwable th ) {
		return key.toString() ;
	    }
	}
	
	private boolean isNeedleHeaderInChain(ArrayList<NeedleHeader> needleHeaderChain, byte[] key) throws NeedleManagerException {
		for ( int i = 0 ; i < needleHeaderChain.size(); i++ ) {
			if ( Arrays.equals(key, needleHeaderChain.get(i).getKeyBytes()) )  
				return true ;
		}
		return false ;
	}
	
	/** Loads the full needle header chain from a given bucket. Only for use by the Voldemort closable iterator
	 * @param initialNeedlePointer
	 * @param key
	 * @return The chain of needles. Target needle is the last in the chain.
	 */
	public ArrayList<NeedleHeader> getFullNeedleHeaderChain(NeedlePointer initialNeedlePointer) throws NeedleManagerException {
		ArrayList<NeedleHeader> result = new ArrayList<NeedleHeader>() ;
		
		NeedleHeader needleHeader = getNeedleHeaderFromCache(initialNeedlePointer) ;
		while ( needleHeader != null && !needleHeader.isNull() ) {
			// Add needle to the chain
			result.add(needleHeader) ;
			// Gets next needle
			NeedlePointer currentPointer = needleHeader.getPreviousNeedle() ;
			if ( currentPointer == null )
				break ; // No more needles
			needleHeader = getNeedleHeaderFromCache(currentPointer.clone()) ; // Fetch next needle
		}
		return result ; // No more needles: we haven't found the requested key
	}
	
	
	/** Loads the first needle header matching the key and returns
	 * @param initialNeedlePointer
	 * @param key
	 * @return
	 */
	public NeedleHeader readNextNeedleHeaderByKey(NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
		return readNextNeedleHeaderByKey(false, initialNeedlePointer, key) ;
	}
	public NeedleHeader readNextNeedleHeaderByKey(boolean noCacheFeeding, NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
		if ( key == null || key.length == 0 )
			return null ; // Stupid developer proof...
		
		NeedleHeader needleHeader = getNeedleHeaderFromCache(initialNeedlePointer) ;
		while ( needleHeader != null && !needleHeader.isNull() ) {
			// Check key
			if ( Arrays.equals(key, needleHeader.getKeyBytes()) )
				return needleHeader ; // This needle is the right one: stop there
			// Gets next needle
			NeedlePointer currentPointer = needleHeader.getPreviousNeedle() ;
			if ( currentPointer == null )
				return null ; // No more needles
			needleHeader = getNeedleHeaderFromCache(currentPointer.clone()) ; // Fetch next needle
		}
		return null ; // No needles found	
	}
	
	/** Loads the first needle header matching the key and returns. Does not fill the cache with results
	 * @param initialNeedlePointer
	 * @param key
	 * @return
	 */
	public NeedleHeader readNextNeedleHeaderByKeyNoCacheFeeding(NeedlePointer initialNeedlePointer, byte[] key) throws NeedleManagerException {
			return readNextNeedleHeaderByKey(true, initialNeedlePointer, key) ;
		}
	
	public Needle getNeedleFromCache(NeedlePointer needlePointer) throws NeedleManagerException {
		try {
			return needleReadCache.get(needlePointer) ;
		} catch ( Throwable th ) {
			throw new NeedleManagerException("Error reading fCachedNeedle", th);
		}
	}
	
	public NeedleHeader getNeedleHeaderFromCache(NeedlePointer needlePointer) throws NeedleManagerException {
		try {
			Chrono chr = new Chrono() ;
			NeedleHeader nh = needleHeaderReadCache.get(needlePointer) ;
			chr.lap("Header from cache", 20) ;
			return nh ;
		} catch ( Throwable th ) {
			throw new NeedleManagerException("Error reading cached NeedleHeader", th);
		}
	}
	
	
	
	/** Loads the needle pointed by the needlePointer and checks for validity (checksum, ...) and returns the next linked needle
	 * @param needlePointer
	 * @param needle
	 * @return a chained needle if the read is successful, otherwise null
	 * @throws NeedleManagerException
	 */
	public Needle getNeedleFromDisk(NeedlePointer needlePointer) throws NeedleManagerException {
		ByteBuffer needleBuffer = null ;
		try {
			
			FileChannel fc = getChannel(needlePointer.getNeedleFileNumber()) ; 
			if ( fc == null )
				return new Needle() ;
			// Position and read needle for check
			long position = needlePointer.getNeedleOffset() ;
			// Acquires a ByteBuffer
			if ( threadBufferQ == null )
				return new Needle() ;
			Chrono chr = new Chrono() ;
			needleBuffer = threadBufferQ.take() ;
			chr.lap("Wait for thread buffer ", 20) ;
			// Finally we have a buffer
			needleBuffer.rewind() ;
			needleBuffer.limit(MAXKEYSIZE+MAXVERSIONSIZE+Needle.NEEDLEOVERHEAD) ;
			
			// First read header to know the data size
			int readBytes = 0, totalHeaderReadBytes = 0;
			while ( readBytes >= 0 && totalHeaderReadBytes < needleBuffer.limit()) {
				readBytes = fc.read(needleBuffer, position+totalHeaderReadBytes) ;
				totalHeaderReadBytes += readBytes;
			}
			if ( totalHeaderReadBytes <= 0 )
				return new Needle() ;
			
			Needle needle = new Needle() ;
			if ( !needle.getNeedleHeaderFromBuffer(needleBuffer) ) {
				return new Needle() ; // Incorrect header
			}
			// Needle Header is OK, read the rest until end of needle. Change limit to include data
			// needleBuffer.rewind() ;
			needleBuffer.position(totalHeaderReadBytes) ;
			// needleBuffer.limit(needle.getPostDataSize()) ;
			needleBuffer.limit(needle.getTotalSizeFromData()) ;
			
			readBytes = 0; 
			int totalContentReadBytes = 0;
			while ( readBytes >= 0 && totalContentReadBytes < needleBuffer.limit()-totalHeaderReadBytes ) {
				readBytes = fc.read(needleBuffer, position+totalHeaderReadBytes+totalContentReadBytes) ;
				totalContentReadBytes += readBytes; 
			}
			// readBytes = fc.read(needleBuffer, position+needle.getHeaderSize()) ;
			// Parse data and verifies checksum
			// 
			needleBuffer.rewind() ;
			needleBuffer.position(needle.getHeaderSize()) ;
			if ( !needle.getNeedleDataFromBuffer(needleBuffer) )
				return new Needle() ;
			// Now needle is parsed and OK
			chr.total("Read from disk ", 20) ;
			return needle ;
		} catch ( Throwable th ) {
			logger.error("Error reading needle at "+needlePointer.getFormattedNeedleFileNumber()+"/"+needlePointer.getFormattedNeedleOffset(), th) ;
			throw new NeedleManagerException()	;
		} finally {
			if ( needleBuffer != null ) {
				try {
					threadBufferQ.put(needleBuffer) ;
				} catch ( InterruptedException ie ){
					throw new BucketTableManagerException("Error giving back needle read thread", ie) ;
				}
			}
		}
	}
	
	public PointedNeedle readFirstNeedleToCompactFromDisk(NeedlePointer needlePointer) throws NeedleManagerException {
		compactNeedle.copyFrom(needlePointer) ;
		if ( needlePointer.isEmpty() ) {
			throw new NeedleManagerException("Attempt to read from empty pointer during cleaning") ;
		}
		return readNextNeedleToCompactFromDisk() ; 
	}
	
	/** Read method for compacting: reads sequentially the log from the preselected needle until end of current files.
	 * Does not feed the cache to keep fresh data in it
	 * @return the next needle or null when end of file
	 * @throws NeedleManagerException
	 */
	public PointedNeedle readNextNeedleToCompactFromDisk() throws NeedleManagerException {
		try {
			long position = -1L ;
			int readBytes = -1 ;
			FileChannel fc = null ;
			
			fc = getChannel(compactNeedle.getNeedleFileNumber()) ; 
			if ( fc == null )
				return null ; 
			// Position and read needle for check
			position = compactNeedle.getNeedleOffset() ;
			compactBuffer.rewind() ;
			compactBuffer.limit(MAXKEYSIZE+MAXVERSIONSIZE+Needle.NEEDLEOVERHEAD) ;
			
			// First read header to know the data size
			int totalHeaderReadBytes = 0;
			readBytes = 0;
			while ( readBytes >= 0 && totalHeaderReadBytes < compactBuffer.limit()) {
				readBytes = fc.read(compactBuffer, position+totalHeaderReadBytes) ;
				totalHeaderReadBytes += readBytes;
			}
			if ( totalHeaderReadBytes <= 0 )
				return null;
			
			// Decode needle
			Needle needle = new Needle() ;
			if ( !needle.getNeedleHeaderFromBuffer(compactBuffer) ) {
				// Incorrect header: truncate file at this position and removes all subsequent files
				throw new NeedleManagerException("Wrong needle header during cleaning at "+compactNeedle.toString()) ; 
			}
			// Needle Header is OK, read the rest until end of needle. Change limit to include data
			compactBuffer.position(totalHeaderReadBytes) ;
			compactBuffer.limit(needle.getTotalSizeFromData()) ;
			
			readBytes = 0; 
			int totalContentReadBytes = 0;
			while ( readBytes >= 0 && totalContentReadBytes < compactBuffer.limit()-totalHeaderReadBytes ) {
				readBytes = fc.read(compactBuffer, position+totalHeaderReadBytes+totalContentReadBytes) ;
				totalContentReadBytes += readBytes; 
			}
			
			compactBuffer.rewind() ;
			compactBuffer.position(needle.getHeaderSize()) ;
			// Parse data and verifies checksum
			if ( !needle.getNeedleDataFromBuffer(compactBuffer) ) {
				// Incorrect data: truncate file at this position and removes all subsequent files
				throw new NeedleManagerException("Wrong needle data during cleaning at "+compactNeedle.toString()) ; 
			}
			// Now needle is parsed and OK
			PointedNeedle pn = new PointedNeedle() ;
			pn.setNeedlePointer(compactNeedle.clone()) ;
			pn.setNeedle(needle) ;
			compactNeedle.positionToNextNeedle(position+needle.getRoundedTotalSize()) ;
			return pn ;
		} catch ( Throwable th ) {
			logger.error("Error reading needle for cleaning at "+compactNeedle.toString(), th) ;
			throw new NeedleManagerException()	;
		} 
	}
	
	public Needle readFirstNeedleToImportFromDisk(int needleFileNumber) throws NeedleManagerException {
		importChannel = getImportChannel(needleFileNumber) ;
		if ( importChannel == null ) {
			logger.info("Could create import channel for import file "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
			throw new NeedleManagerException("Failed creating import read channel "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
		}
		
		importNeedle.setNeedleFileNumber(needleFileNumber) ;
		importNeedle.beginningOfLogFile() ;
		
		return readNextNeedleToImportFromDisk() ; 
	}
	
	public Needle readFirstNeedleToRecoverFromDisk(int needleFileNumber) throws NeedleManagerException {
		importChannel = getRecoveryChannel(needleFileNumber) ;
		if ( importChannel == null ) {
			logger.info("Could create recovery channel for import file "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
			throw new NeedleManagerException("Failed creating recovery read channel "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
		}
		importNeedle.setNeedleFileNumber(needleFileNumber) ;
		importNeedle.beginningOfLogFile() ;
		return readNextNeedleToImportFromDisk() ; 
	}
	
	/** Read method for compacting: reads sequentially the log from the preselected needle until end of current files.
	 * Does not feed the cache to keep fresh data in it
	 * @return the next needle or null when end of file
	 * @throws NeedleManagerException
	 */
	public Needle readNextNeedleToImportFromDisk() throws NeedleManagerException {
		try {
			long position = -1L ;
			int readBytes = -1 ;
			
			if ( importChannel == null )
				return null ; 
			// Position and read needle for check
			position = importNeedle.getNeedleOffset() ;
			importBuffer.rewind() ;
			importBuffer.limit(MAXKEYSIZE+MAXVERSIONSIZE+Needle.NEEDLEOVERHEAD) ;
			
			// First read header to know the data size
			int totalHeaderReadBytes = 0;
			readBytes = 0;
			while ( readBytes >= 0 && totalHeaderReadBytes < importBuffer.limit()) {
				readBytes = importChannel.read(importBuffer, position+totalHeaderReadBytes) ;
				totalHeaderReadBytes += readBytes;
			}
			if ( totalHeaderReadBytes <= 0 ) {
				importChannel.close() ;
				return null ; // End of file`
			}

			// Decode needle
			Needle needle = new Needle() ;
			if ( !needle.getNeedleHeaderFromBuffer(importBuffer) ) {
				// Incorrect header: truncate file at this position and removes all subsequent files
				logger.error("Wrong needle header during import at "+importNeedle.toString()) ; 
				importChannel.close() ;
				return null ;
			}
			// Needle Header is OK, read the rest until end of needle. Change limit to include data
			importBuffer.position(totalHeaderReadBytes) ;
			importBuffer.limit(needle.getTotalSizeFromData()) ;
			
			readBytes = 0; 
			int totalContentReadBytes = 0;
			while ( readBytes >= 0 && totalContentReadBytes < importBuffer.limit()-totalHeaderReadBytes ) {
				readBytes = importChannel.read(importBuffer, position+totalHeaderReadBytes+totalContentReadBytes) ;
				totalContentReadBytes += readBytes; 
			}
			
			importBuffer.rewind() ;
			importBuffer.position(needle.getHeaderSize()) ;
			// Parse data and verifies checksum
			if ( !needle.getNeedleDataFromBuffer(importBuffer) ) {
				// Incorrect data: truncate file at this position and removes all subsequent files
				importChannel.close() ;
				throw new NeedleManagerException("Wrong needle data during import at "+importNeedle.toString()) ; 
			}
		
			importNeedle.positionToNextNeedle(position+needle.getRoundedTotalSize()) ;
			return needle ;
		} catch ( Throwable th ) {
			logger.error("Error reading needle for import at "+importNeedle.toString(), th) ;
			throw new NeedleManagerException()	;
		} 
	}
	
	public PointedNeedle readFirstNeedleFromDiskInLogSequence(NeedlePointer needlePointer, NeedlePointer checkPoint) throws NeedleManagerException {
		repairNeedle.copyFrom(needlePointer) ;
		if ( needlePointer.isEmpty() ) {
			needlePointer.setNeedleFileNumber(0) ;
			needlePointer.setNeedleOffset(0L) ;
		}
		return readNextNeedleFromDiskInLogSequence(checkPoint) ; // Never repairs the checkpointed needle: if this occurs the index file must be rebuild
	}
	
	
	
	/** Read method for repair routines: reads sequentially the log from the given checkpoint until end of all files.
	 * Last file is truncated after the last valid needle (MAGIC numbers and MD5 OK)
	 * @param needlePointer
	 * @param checkPoint No repair will occur for needles <= checkpoint, index should be repaired in this case and checkpoint reseted
	 * @return
	 * @throws NeedleManagerException
	 */
	public PointedNeedle readNextNeedleFromDiskInLogSequence(NeedlePointer checkPoint) throws NeedleManagerException {
		Boolean repairMode = (checkPoint != null) ;
		ByteBuffer needleBuffer = null ;
		int retry = 2 ;
		// System.out.print("0") ;
		try {
			long position = -1L ;
			int readBytes = -1 ;
			int totalHeaderReadBytes = 0;
			FileChannel fc = null ;
			while ( retry > 0 ) {
				retry-- ;
				// System.out.print("a") ;
				fc = getChannel(repairNeedle.getNeedleFileNumber()) ; 
				if ( fc == null )
					return null ;
				// System.out.print("b") ;
				// logger.info("Repairing: reading file "+repairNeedle.toString() ) ;
				// Position and read needle for check
				position = repairNeedle.getNeedleOffset() ;
				// System.out.print("c") ;
				// Acquires a ByteBuffer
				if ( threadBufferQ == null )
					return null ;
				// System.out.println("1") ;
				if ( needleBuffer == null )
					needleBuffer = threadBufferQ.take() ;
				// System.out.println("2") ;
				// Finally we have a buffer
				needleBuffer.rewind() ;
				needleBuffer.limit(MAXKEYSIZE+MAXVERSIONSIZE+Needle.NEEDLEOVERHEAD) ;
				// First read header to know the data size
				totalHeaderReadBytes = 0;
				readBytes = 0;
				while ( readBytes >= 0 && totalHeaderReadBytes < needleBuffer.limit()) {
					readBytes = fc.read(needleBuffer, position+totalHeaderReadBytes) ;
					totalHeaderReadBytes += readBytes;
				}
				if ( totalHeaderReadBytes <= 0 ) {
					if ( !repairMode )
						return null ;
					// End of file, select next file
					if (position == 0 || repairNeedle.positionToNextFile() == null ) {
						// Clean end
						if ( repairNeedle.compareTo(checkPoint) <= 0 ) {
							// We should NEVER repair a checkpointed needle. Kill checkpoint and rebuild index!
							throw new BrokenCheckPointException("Missing checkpointed record "+repairNeedle.toString()) ;
						}
						return null ;
					} else {
						// Continue with next file
						retry = 1 ;
						// System.out.println("-") ;
						logger.info("Reading in sequence: switching to next file, "+repairNeedle.toString()) ;
						continue ;
					}
				} else {
					// We have our needle (good or bad), do not retry
					retry  = 0 ; 
				}
			}
			Needle needle = new Needle() ;
			if ( !needle.getNeedleHeaderFromBuffer(needleBuffer) ) {
				// Incorrect header: truncate file at this position and removes all subsequent files
				if ( !repairMode )
					return null ;
				if ( repairNeedle.compareTo(checkPoint) <= 0 ) {
					// We should NEVER repair a checkpointed needle. Kill checkpoint and rebuild index!
					throw new BrokenCheckPointException("Broken checkpointed record "+repairNeedle.toString()) ;
				}
				truncate(repairNeedle) ;
				return null ; 
			}
			// System.out.println("3") ;
			// Needle Header is OK, read the rest until end of needle. Change limit to include data
			needleBuffer.position(totalHeaderReadBytes) ;
			needleBuffer.limit(needle.getTotalSizeFromData()) ;
			
			readBytes = 0; 
			int totalContentReadBytes = 0;
			while ( readBytes >= 0 && totalContentReadBytes < needleBuffer.limit()-totalHeaderReadBytes ) {
				readBytes = fc.read(needleBuffer, position+totalHeaderReadBytes+totalContentReadBytes) ;
				totalContentReadBytes += readBytes; 
			}
			
			needleBuffer.rewind() ;
			needleBuffer.position(needle.getHeaderSize()) ;
			// Parse data and verifies checksum
			if ( !needle.getNeedleDataFromBuffer(needleBuffer) ) {
				// Incorrect data: truncate file at this position and removes all subsequent files
				if ( !repairMode )
					return null ;
				if ( repairNeedle.compareTo(checkPoint) <= 0 ) {
					// We should NEVER repair a checkpointed needle. Kill checkpoint and rebuild index!
					throw new BrokenCheckPointException("Broken checkpointed record "+repairNeedle.toString()) ;
				}
				// System.out.print("truncate...") ;
				truncate(repairNeedle) ;
				// System.out.print("truncated.") ;
				return null ; 
			}
			// Now needle is parsed and OK
			PointedNeedle pn = new PointedNeedle() ;
			pn.setNeedlePointer(repairNeedle.clone()) ;
			pn.setNeedle(needle) ;
			// System.out.println("4") ;
			// Put needle in cache
			needleReadCache.put(pn.getNeedlePointer(), needle) ;
			// Put needleHeader in cache
			needleHeaderReadCache.put(pn.getNeedlePointer(), needle.getNeedleHeader(pn.getNeedlePointer())) ;
			repairNeedle.positionToNextNeedle(position+needle.getRoundedTotalSize()) ;
			return pn ;
		} catch ( Throwable th ) {
			logger.error("Error reading needle at "+repairNeedle.getFormattedNeedleFileNumber()+"/"+repairNeedle.getFormattedNeedleOffset(), th) ;
			throw new NeedleManagerException()	;
		} finally {
			if ( needleBuffer != null ) {
				try {
					threadBufferQ.put(needleBuffer) ;
				} catch ( InterruptedException ie ){
					throw new BucketTableManagerException("Error giving back needle read thread", ie) ;
				}
			}
		}
	}
	
	private void truncate(NeedlePointer needlePointer) throws NeedleManagerException {
		// Truncate current file
		if ( needlePointer.getNeedleOffset() < 0 ) {
			logger.error("Request for truncating "+needlePointer.toString()+"!!!") ;
			throw new NeedleManagerException("Request for truncating "+needlePointer.toString()+"!!!") ;
		}
		// truncates current file
		FileChannel fc = getChannel(needlePointer.getNeedleFileNumber()) ; 
		if ( fc == null ) {
			logger.error("Request for truncating "+needlePointer.toString()+", no File Channel!!!") ;
			throw new NeedleManagerException("Request for truncating "+needlePointer.toString()+", no File Channel!!!") ;
		}
		try {
			fc.truncate(needlePointer.getNeedleOffset()) ;
			fc.force(true) ;
		} catch ( IOException ie ) {
			logger.error("Error truncating "+needlePointer.toString(), ie) ;
			throw new NeedleManagerException("Error truncating "+needlePointer.toString(), ie) ;
		}
		// Removes subsequent files. In fact just rename them and alert via logger
		int logNumber = needlePointer.getNeedleFileNumber()+1 ;
		while ( channelMap.contains(new Integer(logNumber)) ) {
			closeChannel(logNumber) ;
			File toClose = getFile(logNumber) ;
			if ( toClose != null ) {
				try {
					toClose.renameTo(new File(toClose.getCanonicalFile()+".bak")) ;
				} catch ( IOException ie ) {
					throw new NeedleManagerException("Could not rename to .bak for file number "+Integer.toHexString(logNumber)) ;
				}
			}
			logNumber++ ;
		}
	}
	
	
	
	private static final long TEST_COUNT = 150000L ;
	private static byte[] testData = new byte[10000] ;
	public static void main(String[]args){
		
		try {
			BasicConfigurator.configure() ;
			NeedleManager nw = new NeedleManager("/Users/Francois/Documents/NEEDLES", 1000000000L, 8, 10000000L, 10000000L, 0) ;
			nw.initialize() ;
			
			Runtime runtime = Runtime.getRuntime();
			long before = runtime.totalMemory() - runtime.freeMemory() ;
			
			// Measure cache impact
			for ( long i = 0; i < TEST_COUNT ; i++ ) {
				NeedlePointer np = new NeedlePointer() ;
				np.setNeedleOffset(i) ;
				Needle n = new Needle() ;
				n.setKey("kjdskljsdhklgjfdklgh") ;
				n.setData(new byte[10000]) ;
				nw.needleHeaderReadCache.put(np,n.getNeedleHeader(np)) ;
			}
			runtime = Runtime.getRuntime();
			long after = runtime.totalMemory() - runtime.freeMemory() ;
			System.out.println("Usage="
		            +( (after-before) / TEST_COUNT)+", total= "+( (after-before) / (1024*1024)));
			
			
			/*
			long counter = 0 ;
			int initialLogNumber = nw.getLogNumber() ;
			Date startDate = new Date() ;
			System.out.println("Start test for "+TEST_COUNT+" needles with 10K bytes data") ;
			while ( counter < TEST_COUNT ) {
				Needle needle = new Needle() ;
				needle.setKey("KEY_"+counter) ;
				VectorClock vc = new VectorClock(new Date().getTime()) ;
				needle.setVersion(vc) ;
				needle.setData(testData) ;
				nw.writeNewNeedle(needle) ;
				counter++ ;
				if ( counter % 10000 == 0 )
					System.out.print(".") ;
			}
			long timeSpent = new Date().getTime() - startDate.getTime() ;
			System.out.println("\n\nProcessed creation of "+TEST_COUNT+" needles with 10K bytes data each\n"+
						"\tInital log number: "+initialLogNumber+"\n"+
						"\tFinal log number: "+nw.getLogNumber()+"\n"+
						"\tTotal time: "+timeSpent/1000+"s\n"+
						"\tThroughput: "+TEST_COUNT*testData.length/(timeSpent/1000)+" byte/s\n"+
						"\t            "+TEST_COUNT/(timeSpent/1000)+" needle/s") ;
			*/
		} catch ( Throwable th ) {
			th.printStackTrace() ;
		}
		
	}

	
	public ArrayList<Integer> getFileNumbersToRebuild() throws NeedleManagerException {
        // Browse directory for needle files (won't find any if rebuild required)
		ArrayList<Integer> sortedNumbers = new ArrayList<Integer>() ;
		
		String files[] = importDir.list() ;
        	if ( files != null ) {
			for ( String file : files) {
				// Verifies file name pattern and extracts file number
				int extIndex = file.indexOf(EXTENSION) ;
				if ( extIndex <= 0 )
					continue ;
				int fileNumber = -1 ;
				String hexString = file.substring(0, extIndex) ;
				try {
					fileNumber = Integer.parseInt(hexString, 16) ;
				} catch ( Throwable th ) {
					fileNumber = -1 ;
				}
				if ( fileNumber < 0 ) {
					// Normal situation: non log files (including the bucket file)
					continue ;
				}
				
				File needleFile = new File(importDir,file) ;
				if ( !needleFile.canRead() || !needleFile.canWrite() ) {
					logger.error("No read/write access to "+logPath+"/import/"+file) ;
		        	throw new NeedleManagerException() ;
				}
				
				sortedNumbers.add(new Integer(fileNumber)) ;
			}
			Collections.sort(sortedNumbers) ;
        }   
        return sortedNumbers ;
    }


	public int getLogNumber() {
		return logNumber;
	}
	
	
}
