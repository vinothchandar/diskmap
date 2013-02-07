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
 * @author Francois Vaille
 */
package com.yobidrive.diskmap;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;

import voldemort.utils.ByteArray;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.yobidrive.diskmap.buckets.BucketTableManager;
import com.yobidrive.diskmap.needles.Needle;
import com.yobidrive.diskmap.needles.NeedleHeader;
import com.yobidrive.diskmap.needles.NeedleLogInfo;
import com.yobidrive.diskmap.needles.NeedleManager;
import com.yobidrive.diskmap.needles.NeedlePointer;
import com.yobidrive.diskmap.needles.PointedNeedle;




public class DiskMapManager {
	private String storeName;	//For voldermort interface
	private long syncPeriod ;		// Nb of milliseconds between chekpoints, or 0 for immediate write
	private long checkPointPeriod ;		// Nb of milliseconds between chekpoints, or 0 for immediate write
	private long logSize ; 				// Size of one log file. After reaching this threshold a new file is created
	private int readThreads = 8;// Needle reader threads / Buckets reader threads
	private static Log logger = LogFactory.getLog(DiskMapManager.class);
	// daemon thread for pack
	private short nodeId; 		// node id of storage
	private BucketTableManager btm = null ;
	private NeedleManager needleManager = null ;
	private long needleCachedBytes = 0 ;
	private long needleHeaderCachedBytes = 0 ;
	// private long preloaded = 0 ;
	private long needleNumber = 0L ; // Next needle number
	private NeedlePointer logCheckpoint = null ;
	private NeedlePointer pendingCheckpoint = null ;
	private Object logCheckpointLock = new Object() ;
	// private Object writeNeedleLock = new Object() ;
	private Object pendingCheckpointLock = new Object() ;
	private boolean serviceOK = false ;
	// private HashMap<Long, ReentrantLock> bucketLockMap = new HashMap<Long, ReentrantLock>() ;
	// private ReentrantLock bucketLockMapLock = new ReentrantLock();
	private ReentrantReadWriteLock deleteLogLock = new ReentrantReadWriteLock(true) ;
        private Object serviceLock = new Object() ;
        private TimerTask syncThreadTask = null ;
        private TimerTask cleanerTask = null ;
        //private int keySize ;
        private long lastBtmSync = 0L ;
        private CleanerLauncher cleanerLaucher = null ;
        private short partitionGroup = 0 ;
        private int cachePreload = 0 ;
        private TokenSynchronizer syncsSynchronizer = null ;
        private TokenSynchronizer cleansSynchronizer = null ;
        private DiskMapStore diskMapStore = null ;
	

	public DiskMapManager(String storeName, String logPath, String keyPath, long syncPeriod, long checkPointPeriod, long logSize, int keySize,
			int nbBuffers, int entriesPerBuffer, int packInterval, int readThreads, long needleHeaderCachedBytes, long needleCachedBytes, 
			boolean useAverage, short nodeId, short partitionGroup, int cachePreload,
			TokenSynchronizer syncsSynchronizer, TokenSynchronizer cleansSynchronizer, DiskMapStore diskMapStore) {
		this.storeName = storeName;
		// this.logPath = logPath;
		// this.keyPath = keyPath ;
		this.logSize = logSize ;
		// this.keySize = keySize ;
		this.readThreads = readThreads ;
		this.needleHeaderCachedBytes = needleHeaderCachedBytes ;
		this.needleCachedBytes = needleCachedBytes ;
		this.nodeId = nodeId;
		this.syncPeriod = syncPeriod ;
		this.checkPointPeriod = checkPointPeriod ;
		this.partitionGroup =  partitionGroup ;
		this.cachePreload = cachePreload ;
		this.syncsSynchronizer = syncsSynchronizer ;
		this.cleansSynchronizer = cleansSynchronizer ;
		this.diskMapStore = diskMapStore ;
		
		// Create bucket manager
		btm = new BucketTableManager(keyPath+"/"+nodeId+"."+partitionGroup+"."+storeName, nbBuffers, entriesPerBuffer, checkPointPeriod, useAverage) ;
		
		// Create needle manager
		needleManager = new NeedleManager(logPath+"/"+nodeId+"."+partitionGroup+"."+storeName, this.logSize, this.readThreads, this.needleHeaderCachedBytes, this.needleCachedBytes, this.cachePreload) ;
		needleManager.initialize() ;
		btm.initialize() ;
		
		// First do standard repair from checkpoint (no bucket structure change)
		repairAndPreload(btm.getCheckPoint()) ;
		
		// Process files to rebuild as a batch input
		
		// TODO Manage pre-opening for import only
		logger.info("Storage engine for store "+storeName+ " / zone "+partitionGroup+" ready") ;
		openService() ;
		
	}
	
	public void finalizeInit() {
		importBulkInput() ;
		
		if ( logger.isTraceEnabled() ) {
			logger.trace("Logger trace active");
		}
		
		startSyncTask(this.syncPeriod) ;
		
		cleanerLaucher = new CleanerLauncher(this) ;
		startCleanerTask() ;
		// for ( int i = 0; i < 6 ; i++) statList.add(0L);
		// recentCacheStats = new RecentCacheStats(this);
		// hook shutdown process to close all file and flush all delay writes
		// Runtime.getRuntime().addShutdownHook( new Thread( new Shutdown()));
	}

	
	public void assertOpenService() throws DiskMapManagerException {
		if ( !isServiceOK() )
			throw new DiskMapManagerException("Service closed for store "+storeName) ;
	}
	
	
	/** Repairs and preloads: repair+preload from log file containing checkpoint, then preloads previous logs until cache filled
	 * @param lastCheckPoint
	 * @param maxPreloadedEntries
	 * @throws DiskMapManagerException
	 */
	private void repairAndPreload(NeedlePointer lastCheckPoint) throws DiskMapManagerException {
		// First preloads and repair from file containing the checkpoint
		NeedlePointer preloadPointer = lastCheckPoint.clone() ;
		// preloaded = 0 ;
		needleNumber = 0L ;
		long targetCached = 0L ;
		// Loads the checkpointed needle
		if ( lastCheckPoint.isEmpty() ) {
			// No checkpoint yet: walk down ALL the logs
			logger.warn("No checkpoint yet: read all log files") ;
			// Points to oldest log file
			int firstFile = needleManager.getFirstFileNumber() ;
			preloadPointer.setNeedleFileNumber(firstFile) ;	
		} else {
			// Gets the maximum before checkpoint to fill the cache
			Needle checkPointedNeedle = needleManager.getNeedleFromDisk(lastCheckPoint) ;
			if ( checkPointedNeedle == null || checkPointedNeedle.isNull() )
				throw new DiskMapManagerException("Missing last checkpoint "+lastCheckPoint.toString()+" in needle log files") ;
		}
		
		// Preloads from the log containing last checkpoint, starting from beginning of file, and repairs after checkpoint
		logger.info("Repairing after checkpoint "+lastCheckPoint.toString()+"...") ;
		if ( lastCheckPoint==null || lastCheckPoint.isEmpty() )
			preloadPointer.beginningOfLogFile() ; 
		repairFromCheckpointFile(preloadPointer, lastCheckPoint) ;
	}

	/** Repair: reads log files from a starting checkpoint and process index update
	* @param preloadPointer the starting point for preloading
	* @param lastCheckPoint the mast checkpoint (repair will only occur for needles > lastCheckpoint)
	* @return false if no file found for preloading, true in all other cases
	* @throws DiskMapManagerException
	*/
	private boolean repairFromCheckpointFile(NeedlePointer preloadPointer, NeedlePointer lastCheckPoint) throws DiskMapManagerException {
		long counter = 0 ;
		try {
			PointedNeedle pointedNeedle = needleManager.readFirstNeedleFromDiskInLogSequence(preloadPointer, lastCheckPoint) ;
			if ( lastCheckPoint == null && pointedNeedle == null )
				return false ; // Preload mode and no needle found: returns false to stop the process
			while ( pointedNeedle != null ) {
				if ( lastCheckPoint != null )
					needleNumber = pointedNeedle.getNeedle().getNeedleNumber() + 1;
				if (  lastCheckPoint == null || pointedNeedle.getNeedlePointer().compareTo(lastCheckPoint) <= 0) {
					// Already checkpointed, or just a preloading sequence on a log file before the checkpoint 
					// btm.getNeedlePointer(btm.hash(pointedNeedle.getNeedle().getKeyBytes())) ;
					// preloaded++ ;
				} else {
					long hash = btm.hash(pointedNeedle.getNeedle().getKeyBytes()) ;
					// Before writing to the index, checks that the included previous Needle is OK
					/* if ( !checkLoggedNeedle(hash, pointedNeedle.getNeedle().getKeyBytes(), pointedNeedle.getNeedle().getVersion(), 
										pointedNeedle.getNeedlePointer(), pointedNeedle.getNeedle().getPreviousNeedle()) 
						&& compactRequiredFrom == null) {
						logger.error("Compacting required: broken needle chain at "+pointedNeedle.getNeedlePointer()) ;
						compactRequiredFrom = pointedNeedle.getNeedlePointer().clone() ;
					} */
					// Write needle pointer to index
					btm.writeNeedlePointer(hash, pointedNeedle.getNeedlePointer()) ;
					btm.moveNeedle(pointedNeedle.getNeedle().getOriginalFileNumber(), pointedNeedle.getNeedle().getOriginalSize(), pointedNeedle.getNeedlePointer().getNeedleFileNumber(), pointedNeedle.getNeedle().getRoundedTotalSize()) ;
					counter++ ;
				}
				if ( lastCheckPoint != null )
					setLogCheckpoint(pointedNeedle.getNeedlePointer()) ;
				pointedNeedle = needleManager.readNextNeedleFromDiskInLogSequence(lastCheckPoint) ;
			}
			
			if ( lastCheckPoint != null && counter > 0 ) {
				logger.info("Repaired "+counter+ " needles") ;
			} 
		} catch ( Throwable th ) {
			logger.fatal("Repair failed from "+lastCheckPoint.toString()) ;
			throw new DiskMapManagerException("Repair failed", th) ;
		}
		return true ;
	}


	
	
	/** Reads all versions for one key. In fact reads the last and only version kept.
	 * @param key
	 * @return
	 * @throws DiskMapManagerException
	 */
	public  List<Versioned<byte[]>> get(byte[] key) throws DiskMapManagerException {
		assertOpenService() ;
		List<Versioned<byte[]>> results = Lists.newArrayList();
		long hash = btm.hash(key) ;
		NeedlePointer needlePointer = btm.getNeedlePointer(hash) ; // Gets bucket
		if ( needlePointer != null && !needlePointer.isEmpty() ) { // needles behind bucket
			// Get first key in chain
			Needle needle = needleManager.readNextNeedleByKey(needlePointer, key) ; // Get first needle matching key
			if ( needle != null && !needle.isDeleted()) { // Found a needle
				Versioned<byte[]> lastVersion = new Versioned<byte[]>(needle.getNotNullData(), needle.getVersion()) ;
				results.add(lastVersion) ; // Add voldemort versioned object
			} else {
				// System.out.println("not found "+key.toString()) ;
			}
		}
		return results; // Return the list (with one element in our case)
	}
	
	/** Reads all versions for one key. In fact reads the last and only version kept.
	 * @param key
	 * @return
	 * @throws DiskMapManagerException
	 */
	public  List<Version> getVersions(byte[] key) throws DiskMapManagerException {
		assertOpenService() ;
		List<Version> results = Lists.newArrayList();
		long hash = btm.hash(key) ;
		NeedlePointer needlePointer = btm.getNeedlePointer(hash) ; // Gets bucket
		if ( needlePointer != null && !needlePointer.isEmpty() ) { // needles behind bucket
			// Get first key in chain
			NeedleHeader needleHeader = needleManager.readNextNeedleHeaderByKey(needlePointer, key) ; // Get first needle matching key
			if ( needleHeader != null && !needleHeader.isDeleted()) { // Found a needle
				Version lastVersion = needleHeader.getVersion() ;
				results.add(lastVersion) ; // Add voldemort versioned object
			}
		}
		return results; // Return the list (with one element in our case)
	}
	


	
	private boolean isServiceOK() {
		 synchronized (serviceLock) {
			return serviceOK;
		}
	}

	
	private void openService() {
		synchronized (serviceLock) {
			this.serviceOK = true;
		}
	}
	
	private void closeService() {
		synchronized (serviceLock) {
			this.serviceOK = false;
		}
	}
	
	/** Sync log to disk and optionnaly commits index
	 * @param commit
	 */
	
	
	public void stopService() throws DiskMapManagerException {
		try {
			if ( syncThreadTask != null ) syncThreadTask.cancel();
			sync(false) ;
		} catch ( Throwable th ) {
			logger.error("Error stopping service for store "+storeName, th) ;
		}
	}
	
	public void sync() {
		sync(false) ;
	}
	
	public void startSyncTask(long period) {
        logger.info("Sync logs every "+period+" ms");
        lastBtmSync = new Date().getTime() ;
        if ( syncThreadTask != null ) syncThreadTask.cancel() ;
        syncThreadTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    sync() ;
                } catch (Exception ex) {
                    //swallow exception
                    logger.error( ex.getMessage(), ex);
                }
            }
        } ;
        new Timer().schedule( syncThreadTask, period , period);
    }
	
	public void startCleanerTask() {
        lastBtmSync = new Date().getTime() ;
        if ( cleanerTask != null ) cleanerTask.cancel() ;
        cleanerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                	startClean() ;
                } catch (Exception ex) {
                    //swallow exception
                    logger.error( ex.getMessage(), ex);
                }
            }
        } ;
        //FIXME: Put it again later
        //new Timer().schedule( cleanerTask, 5000 , 5000);
    }
	
	private void startClean() {
		if ( cleanerLaucher.isCleanerRunning() ) {
				return ;
		}
		cleanerLaucher.startCleaner() ;
	}
	
	public void stopClean() {
		cleanerLaucher.waitForCleanerStop() ;
	}
	
	
	
	/** Cleans one full log file by rewriting live needles at the end of the log, then closing the channel and removing the log file
	 * @param needleFileNumber
	 * @throws DiskMapManagerException
	 */
	public boolean cleanLogFile(NeedleLogInfo nli) throws DiskMapManagerException {
	    Integer token = null ;
	    try {
	    	token = cleansSynchronizer.getToken() ;
	    	btm.getBucketShuttleLauncher().startShuttle(getLogCheckpoint()) ; 
	    	
	    
	    
	    
		int needleFileNumber = nli.getNeedleFileNumber() ;
		if ( !needleManager.checkChannel(needleFileNumber) ) {
			getBtm().removeLogInfo(needleFileNumber) ; // During repair this is a normal situation. Clean log info to save memory.
			logger.warn("File already deleted "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
			return false ;
		}
		logger.warn("Cleaning "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)+" utilisation="+nli.getUtilization()+", average age="+nli.getAverageDOB()+", yougest="+nli.getYougestDOB()+",B/C="+nli.getBenefitOnCost()) ;

		int processed = 0 ;
		int cleaned = 0 ;
		NeedlePointer needlePointer = new NeedlePointer() ;
		needlePointer.beginningOfLogFile() ;
		needlePointer.setNeedleFileNumber(needleFileNumber) ;
		
		// Walk down the log file and copy the non deleted files
		PointedNeedle pointedNeedle = needleManager.readFirstNeedleToCompactFromDisk(needlePointer) ;
		while ( pointedNeedle !=  null ) {
			processed++ ;
			boolean gotoNext = false ;
			NeedlePointer currentBTMPointer = btm.getNeedlePointer(btm.hash(pointedNeedle.getNeedle().getKeyBytes())) ;
			if ( currentBTMPointer == null || currentBTMPointer.isEmpty() ) {
				cleaned++ ;
				gotoNext = true ; // No needles at all anymore: deleted => skip
			} else if ( !currentBTMPointer.equals(pointedNeedle.getNeedlePointer()) ) {
				// This is not ourselves
				NeedleHeader mostRecentNeedleHeader = needleManager.readNextNeedleHeaderByKeyNoCacheFeeding(currentBTMPointer, pointedNeedle.getNeedle().getKeyBytes()) ;
				if ( mostRecentNeedleHeader == null ) {
					cleaned++ ;
					gotoNext = true ; // No needles at all anymore: deleted => skip
				} else if ( mostRecentNeedleHeader.getNeedleAddress().compareTo(pointedNeedle.getNeedlePointer()) > 0 ) {
					cleaned++ ;
					gotoNext = true ; // Recent needle is actually more recent => skip
				} else if ( mostRecentNeedleHeader.getNeedleAddress().compareTo(pointedNeedle.getNeedlePointer()) < 0 ) {
					throw new DiskMapManagerException("Recent "+mostRecentNeedleHeader.getNeedleAddress().toString()+" < cleaned "+pointedNeedle.getNeedlePointer().toString()+" for key "+pointedNeedle.getNeedle().getKey()+" (stored pointer="+currentBTMPointer.toString()+")") ;
				}
			}
			if ( !gotoNext ) {
				// All other cases: most recent is ourselves: rewrite the needle without changing vector
				if ( pointedNeedle.getNeedle().isDeleted() ) {
					// TODO: The last needle is a deleted one:take advantage of compacting to remove it
					deleteNoVersionCheck(pointedNeedle.getNeedle().getKeyBytes(), pointedNeedle.getNeedle().getVersion()) ; 
				} else {
					putNoVersionCheck(pointedNeedle.getNeedle().getKeyBytes(), pointedNeedle.getNeedle().getData(), pointedNeedle.getNeedle().getVersion()) ; 
				}
			}
			pointedNeedle = needleManager.readNextNeedleToCompactFromDisk() ;
		}
		// request log sync before deleting compacted file
		syncLogs() ;
		// delete old file
		removeLogFile(needleFileNumber) ;
		logger.warn("Recovered "+cleaned+" needles / "+processed+", deleted file "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
		return true ;
	    } catch ( DiskMapManagerException dme ) {
	    	    throw dme ;
	    } finally {
	    	if ( token != null )
	    	    cleansSynchronizer.putToken(token) ;
	    }
	}
	
	
	/** Imports .ydr files: process as input data (used for rebuild), discarding chaining information
	 * @throws DiskMapManagerException
	 */
	private void importBulkInput() throws DiskMapManagerException {
		ArrayList<Integer> fileNumbers = needleManager.getFileNumbersToRebuild() ;
		int processed = 0 ;
		for ( int i = 0 ; i < fileNumbers.size(); i++ ) {
			if ( !importLogFile(fileNumbers.get(i)) ) {
				break ;
			} else
				processed++ ;
		}
		logger.info("Recovered "+processed+" files") ;
	}
	
	
	/** Imports one full log file by rewriting live needles as new entries, then closing and removing the log file
	 * @param needleFileNumber
	 * @throws DiskMapManagerException
	 */
	private boolean importLogFile(int needleFileNumber) throws DiskMapManagerException {
	    	if ( diskMapStore == null ) {
	    	    logger.error("No imports will be done: no DiskMapStore passed") ;
	    	    return true ;
	    	}
		FileChannel importChannel = needleManager.getImportChannel(needleFileNumber) ;
		if ( importChannel == null ) {
			logger.info("Could create import channel for import file "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
			return false ;
		}
		logger.info("Importing "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)+"...") ;

		int processed = 0 ;
	
		
		// Walk down the log file and copy the non deleted files
		Needle needle = needleManager.readFirstNeedleToImportFromDisk(needleFileNumber) ;
		while ( needle !=  null ) {
			
			
			// All other cases: most recent is ourselves: rewrite the needle without changing vector
			if ( needle.isDeleted() ) {
				// TODO: The last needle is a deleted one:take advantage of compacting to remove it
				diskMapStore.deleteNoVersionCheck(new ByteArray(needle.getKeyBytes()), needle.getVersion()) ; 
			} else {
			    	diskMapStore.putNoVersionCheck(new ByteArray(needle.getKeyBytes()), needle.getData(),needle.getVersion()) ; 
			}
			processed++ ;
			needle = needleManager.readNextNeedleToImportFromDisk() ;
		}
		// request log sync before deleting compacted file
		syncLogs() ;
		// delete old file
		needleManager.removeImportFile(needleFileNumber) ;

		logger.info("Imported "+processed+" needles, deleted file "+NeedlePointer.getFormattedNeedleFileNumber(needleFileNumber)) ;
		return true ;
	}

	private /* synchronized */ void removeLogFile(int needleFileNumber) {
	    	getLogDeleteLock() ;
		needleManager.removeLogFile(needleFileNumber) ;
		btm.removeLogInfo(needleFileNumber) ;
		releaseDeleteLogLock() ;
	}
	
	
	
	/** Sync logs and sync buckets if not yet running
	 * @param force requests a second loop to commit last buckets. Could be optimized (shutdown can take 1 hour at worst).
	 */
	private void sync(boolean force) {
		syncLogs() ;
		long now = new Date().getTime() ;
		boolean shouldSyncIndex = (now-lastBtmSync) > checkPointPeriod ;
		if ( !force && !shouldSyncIndex )
			return ;
		if ( btm.getBucketShuttleLauncher().isShuttleRunning() ) {
			if ( force )
				btm.getBucketShuttleLauncher().waitForShuttleStop() ;
			else
				return ;
		}
		    
		
		lastBtmSync = now ;
		if ( getLogCheckpoint() != null && !getLogCheckpoint().isEmpty() ) {
		    Integer token = null ;
		    try {
			token = syncsSynchronizer.getToken() ;
			btm.getBucketShuttleLauncher().startShuttle(getLogCheckpoint()) ; 
		    } catch ( Throwable th ) {
			logger.error("Error synching "+btm.getKeyDirPath(), th) ;
		    } finally {
			if ( token != null )
			    syncsSynchronizer.putToken(token) ;
		    }
		}
	}
	
	private void syncLogs() {
			NeedlePointer futureLogCheckpoint = getPendingCheckpoint() ; // Freeze before sync in case writes come between sync and 
			needleManager.syncAll() ;
			
			if ( !futureLogCheckpoint.isEmpty() ) {
				// System.out.println("Sync log "+futureLogCheckpoint.toString()) ;
				setLogCheckpoint(futureLogCheckpoint) ;
			}
	}
	
	/** Puts data with key, value, version and flag with version checking (if version != null). 
	 * Concurrent versions are resolved based on timestamp so that the ring never has to manage concurrent versions
	 * @param noVersionCheck if true, accept put for any version
	 * @param key
	 * @param data
	 * @param value
	 * @return true if put processed, false if obsolete version
	 * @throws DiskMapManagerException
	 */
	private boolean put(boolean noVersionCheck, byte[] key, byte[] data, Version version) throws DiskMapManagerException {
		return putWithFlags(noVersionCheck, key, data, version, false) ; 
	}
	public boolean put(byte[] key, byte[] data, Version version) throws DiskMapManagerException {
		return put(false, key, data, version) ;
	}
	public boolean putNoVersionCheck(byte[] key, byte[] data, Version version) throws DiskMapManagerException {
		return put(true, key, data, version) ;
	}
	
	private String getStringFromBytes(byte[] key) {
		try {
			return new String (key, "UTF-8") ;
		} catch ( Throwable th ) {
			
		}
		return "" ;
	}
	
	/** Puts data with key, value, version and delete flag with version checking (if version != null). 
	 * Concurrent versions are resolved based on timestamp so that the ring never has to manage concurrent versions
	 * @param noVersionCheck if true, accept put/delete for any version
	 * @param key
	 * @param data
	 * @param value
	 * @return true if put processed, false if obsolete version. In case of delete flag returns true is something has been deleted
	 * @throws DiskMapManagerException
	 */
	public /* synchronized */ boolean putWithFlags(boolean noVersionCheck, byte[] key, byte[] data, Version version, boolean delete) throws DiskMapManagerException {
		if ( key == null )
			throw new DiskMapManagerException("Null key in put") ;
		if ( version == null )
			throw new DiskMapManagerException("Null version in put") ;
//		String trace = "" ;
//		try {
//			trace = new String (data, 1, data.length-1,"UTF-8") ;
//		} catch ( Throwable th ) {
//			trace = "" ;
//		}
		assertOpenService() ; 
		// Reads last version
		long hash = btm.hash(key) ;
		// Acquires a lock on bucket
		ReentrantLock lock = getBucketLock(hash) ;
		Chrono chr = new Chrono() ;
		NeedlePointer storedPointer = btm.getNeedlePointer(hash) ;
		NeedlePointer needlePointer = storedPointer==null?null:storedPointer.clone() ;
		ArrayList<NeedleHeader> needleHeaderChain = new  ArrayList<NeedleHeader>() ;
		int positionInChain = -1 ;
		// Read 
		Version dbVersion = null ;
		if ( needlePointer != null  ) {
			// There are some needles in this bucket: gets the chain to the key
			// Get first key in chain
			positionInChain = needleManager.feedNeedleHeaderChain(needleHeaderChain, needlePointer, key) ;
			
			if ( positionInChain >= 0 ) {
				// Gets the needle header of the target
				NeedleHeader needleHeader = needleHeaderChain.get(positionInChain) ;
				// There is a needle with the same key: compare versions
				dbVersion = needleHeader.getVersion() ;
				boolean dbDeleted = needleHeader.isDeleted() ;
				if ( !noVersionCheck ) {
					//if ( "updatesystemts:::WRITER:::SEQUENCE".equals(getStringFromBytes(key) ))
					//	System.out.println("DMAP: "+(delete?" DELETE ":" PUT ")+getStringFromBytes(key)+" = "+version.toString()+", dbversion = "+dbVersion.toString()+", data="+trace) ;
					if ( dbVersion != null ) {
						if ( delete ) {
							// Delete up to requested version INCLUDED (according to BEFORE relationship)
							if ( dbVersion.compare(version) == Occurred.BEFORE ) {
								// Db Version is OLDER: OK
								// if ( delete ) System.out.println("DMAP: DB Version is BEFORE, do delete "+getStringFromBytes(key)+" = "+version.toString()+", dbversion = "+dbVersion.toString()) ;
							} else if ( dbVersion.compare(version) == Occurred.AFTER ) {
								// OBSOLETE for sure
								if ( dbDeleted ) {
									return releaseBucketLock(hash, lock, true) ;  // Already deleted with more recent version, do nothing and say deleted
									// else the put has precedence over a delete, accept it (voldemort doesn't manage versionned deletions yet)
								} else {
									// if ( delete ) System.out.println("DMAP: DB Version is AFTER, do not delete "+getStringFromBytes(key)+" = "+version.toString()+", dbversion = "+dbVersion.toString()) ;
								    return releaseBucketLock(hash, lock, false) ; // Put or delete obsolete compared to existing needle, return obsolete
								}
							} else if ( dbVersion.compare(version) == Occurred.CONCURRENTLY ) {
								// Don't know yet: check timestamps. Never return obsolete in this case, just discard the oldest one silently								
								if ( dbDeleted )
									return releaseBucketLock(hash, lock, true) ;  // Already deleted by a concurrent version, say deleted
								else
									return releaseBucketLock(hash, lock, false) ; // Existing needle, only accepts deletes if BEFORE, so reject
							}
							// End deletion case
						} else {
							// Put if exclusively AFTER (according to AFTER relationship)
							if ( version.compare(dbVersion) == Occurred.AFTER ) {
								// Db Version is OLDER: OK
							} else if ( version.compare(dbVersion) == Occurred.BEFORE ) {
								// OBSOLETE for sure
								if ( !dbDeleted ) {
									return releaseBucketLock(hash, lock, false) ; // Reject obsolete records over live data (not deleted)
								}
							} else if ( version.compare(dbVersion) == Occurred.CONCURRENTLY ) {
								// Don't know yet: check timestamps. Never return obsolete in this case, just discard the oldest one silently
							    	if ( !dbDeleted ) { // FVE 20120810 Concurrency resolving bug fix
								    // Accept new data over a deleted record
							    	    return releaseBucketLock(hash, lock, false) ; // Reject concurrent records over live data (not deleted)
								} 
							    /* FVE 20120810 Concurrency resolving bug fix
							    	long ts = 0L ;
								if ( version instanceof VectorClock ) {
									ts = ((VectorClock) version).getTimestamp() ;
								}
								long dbTs = 0L ;
								if ( dbVersion instanceof VectorClock ) {
									dbTs = ((VectorClock) dbVersion).getTimestamp() ;
								}
								if ( ts <= dbTs ) {
									// Obsolete by ts 
									return releaseBucketLock(hash, lock, false) ; 
								}
							    */
							}
							// End put case
						} 
					} // dbVersion != null
				}
				// Other cases: Occurred AFTER or CONCURRENTLY WITH more recent timestamp, or no version check
			}
		}

		if ( delete && (needlePointer == null || needlePointer.isEmpty()) ) {
			return releaseBucketLock(hash, lock, false) ; // Nothing to delete
		}

		int copyStart = 0 ; 				// We 'll rewriting from this position
		int copyEnd = 0 ; 					// We'll stop rewriting at that position
		// Determine rewrite strategy and prepares initial pointer to next needle in chain
		int chainSize = needleHeaderChain.size() ;
		
		if ( positionInChain < 0 ) {
			// Strategy = add to chain (or create chain): does not exist yet
			copyStart = -1 ;
			copyEnd = -1 ;
			positionInChain = -1 ; // to be sure!
			if ( chainSize > 0 && logger.isTraceEnabled() )
			    logger.trace("Chain size for key "+getStringFromKey(key)+" = "+chainSize+1) ;
		} else if ( needleHeaderChain.size() == 1 ) {
			// Strategy = Rewrite (only needle, take the place, pointing to nothing
			if ( positionInChain != 0 )
				throw new DiskMapManagerException("REWRITE strategy choosen for "+key.toString()+" with non-zero position in chain "+positionInChain) ;
			needlePointer = null ;
			copyStart = 0 ;
			copyEnd = 0 ;
			if ( chainSize > 1 && logger.isTraceEnabled() )
			    logger.trace("Chain size for key "+getStringFromKey(key)+" = "+chainSize) ;
		} else /* if ( positionInChain < (needleHeaderChain.size()/2) ) */ { 
			// Strategy = Top to Target
			if ( (positionInChain + 1) >= needleHeaderChain.size() ) {
				// throw new DiskMapManagerException("TOP_TO_TARGET strategy choosen for "+key.toString()+" with no next needle after "+positionInChain) ;
			    	// Start pointing to null needle
			    needlePointer = null ;
			} else {
			    needlePointer = needleHeaderChain.get(positionInChain + 1).getNeedleAddress()  ; // Point to second half of chain
			}
			copyStart = 0 ;
			copyEnd = positionInChain ;
			if ( chainSize > 1 && logger.isTraceEnabled() )
			    logger.trace("Chain size for key "+getStringFromKey(key)+" = "+chainSize) ;
		} /* else {
			// Strategy = Target to End		
			if ( positionInChain >= needleHeaderChain.size() )
				throw new DiskMapManagerException("TARGET_TO_END strategy choosen for "+key.toString()+" with needle after end of chain"+positionInChain) ;
			needlePointer = needleHeaderChain.get(0).getNeedleAddress()  ; // Point to first half of chain
			copyStart = positionInChain ;
			copyEnd = needleHeaderChain.size() - 1 ;
			if ( chainSize > 1 && logger.isTraceEnabled() )
			    logger.trace("Chain size for key "+getStringFromKey(key)+" = "+chainSize) ;
		} */
		// Rewrite all needle from start to end, writing the new data for the position In Chain
		for ( int i = copyStart ; i <= copyEnd; i++ ) {
			// Prepares needle
			Needle needle ;
			// NeedlePointer originalPointer = null ;
			int originalFileNumber = -1 ;
			int originalSize = 0 ;
			
			if ( positionInChain == i ) {
				// Written needle
				if ( i >= 0 ) { // Existing needle replace by new version or deleted
					originalFileNumber = needleHeaderChain.get(i).getNeedleAddress().getNeedleFileNumber() ;
					originalSize = needleHeaderChain.get(i).getRoundedTotalSize() ;
				}
				needle = new Needle() ;
				needle.setKeyBytes(key) ;
				needle.setData(delete?null:data) ;
				needle.setVersion((VectorClock) version) ;
				needle.setDeleted(delete) ;
			} else {
				// Rewrite of existing needle
				originalFileNumber = needleHeaderChain.get(i).getNeedleAddress().getNeedleFileNumber() ;
				needle = needleManager.getNeedleFromCache(needleHeaderChain.get(i).getNeedleAddress()) ;
				if ( needle == null || needle.isNull() ) 
					throw new DiskMapManagerException("Empty needle at "+needleHeaderChain.get(i).getNeedleAddress().toString()+" behind stored "+storedPointer.toString()) ;
				originalSize = needle.getRoundedTotalSize() ;
			}
			needle.setPreviousNeedle(needlePointer) ;
			
			
			// synchronized (writeNeedleLock) {
        		// Critical section for (re)writing a needle
        		needle.setNeedleNumber(needleNumber) ;
        		needle.setOriginalFileNumber(originalFileNumber) ; 	// For repairing cleaning info
        		needle.setOriginalSize(originalSize) ;															// For repairing cleaning info
        		// Prepares write and gets new needle pointer
        		needlePointer = needleManager.prepareWriteNeedle(needle) ;
        		needleManager.commitWrite(needle) ;
        		needleNumber++ ;
        		setPendingCheckpoint(needlePointer) ;
        			
        		// Updates clean ( utilizations ) counters
        		btm.moveNeedle(originalFileNumber, originalSize, needlePointer.getNeedleFileNumber(), needle.getRoundedTotalSize()) ;
    			

		}
		// Writes bucket pointing to top of chain
		btm.writeNeedlePointer(hash, needlePointer) ; // write new hash index
		chr.total("Total ", 100) ;
		
		// Release write lock for bucket
		return releaseBucketLock(hash, lock, true) ;
		// return true ;
	}
	
	private String getStringFromKey(byte[] key) {
	    try {
		return new String(key,"UTF-8") ;
	    } catch ( Throwable th) {
		return new String(key) ;
	    }
	}
	
	private void getLogDeleteLock() {
	    deleteLogLock.writeLock().lock() ;
	}
	
	/** Locks a bucket for writing
	 * @param hash
	 * @return
	 */
	private ReentrantLock getBucketLock(long hash) {
	    
	    // First check the delete lock
/*
		deleteLogLock.readLock().lock() ;
		
		
		ReentrantLock lock = null ;
		// Make sure nobody else is playing with the lock table
		bucketLockMapLock.lock() ;

		// Update the lock table if required
		Long bucket = new Long(hash) ;
		    
		// Creates the lock if not existing in the table
		while ( bucketLockMap.containsKey(bucket) ) {
		    lock = bucketLockMap.get(bucket) ;	// Get lock from table
		    bucketLockMapLock.unlock() ;		// Release write lock
		    lock.lock() ;				// And wait for lock
		    bucketLockMapLock.lock() ;		// Recheck table
		    lock.unlock() ;				// And do nothing from lock: we want our own table entry
		} 
	    	lock = new ReentrantLock() ;
	    	lock.lock() ; 				// Acquire new lock before publishing
	    	bucketLockMap.put(bucket, lock) ; 	// publish lock
	    	bucketLockMapLock.unlock() ; 		// release table lock   
		
	    	
	    	
		return lock ;
		*/
	    deleteLogLock.writeLock().lock() ;
	    return null ;
	}
	
	private boolean releaseBucketLock(long hash, ReentrantLock lock, boolean returnValue) {    
	    /*
	    
	    // Make sure nobody else is playing with the lock table
	    bucketLockMapLock.lock() ;
	    // Remove lock from table
	    bucketLockMap.remove(new Long(hash)) ;
	    // Release lock table lock
	    bucketLockMapLock.unlock() ;
	    // Release lock
	    if ( lock.isHeldByCurrentThread() )
		lock.unlock() ;
	    else
		throw new DiskMapManagerException("Trying to release lock held by another thread for bucket "+hash) ;
	    
	    deleteLogLock.readLock().unlock() ;
	    
	    return returnValue ;
	    
	    */
	    deleteLogLock.writeLock().unlock() ;
	    return returnValue ;
	}
	
	private void releaseDeleteLogLock() {
	    deleteLogLock.writeLock().unlock() ;
	}
	
	 

	
	
	/** Deletes element key/version if not obsolete
	 * @param noVersionCheck if true accept delete for any version
	 * @param key
	 * @param version
	 * @return
	 * @throws DiskMapManagerException
	 */
	private boolean delete(boolean noVersionCheck, byte[] key, Version version) throws DiskMapManagerException {
		return putWithFlags(noVersionCheck, key, null, version, true) ;
	}
	
	public boolean delete(byte[] key, Version version) throws DiskMapManagerException {
		return delete(false, key, version) ;
	}
	
	public boolean deleteNoVersionCheck(byte[] key, Version version) throws DiskMapManagerException {
		return delete(true, key, version) ;
	}
	

	public BucketTableManager getBtm() {
		return btm;
	}

	public NeedleManager getNeedleManager() {
		return needleManager;
	}

	public short getNodeId() {
		return nodeId;
	}



	public NeedlePointer getPendingCheckpoint() {
		synchronized (pendingCheckpointLock) {
			return pendingCheckpoint==null?new NeedlePointer():pendingCheckpoint.clone();
		}
	}



	public void setPendingCheckpoint(NeedlePointer pendingCheckpoint) {
		synchronized (pendingCheckpointLock) {
			this.pendingCheckpoint = pendingCheckpoint==null?null:pendingCheckpoint.clone();
		}
	}



	public NeedlePointer getLogCheckpoint() {
		synchronized (logCheckpointLock) {
			return logCheckpoint==null?new NeedlePointer():logCheckpoint.clone();
		}
	}



	public void setLogCheckpoint(NeedlePointer logCheckpoint) {
		synchronized (logCheckpointLock) {
			this.logCheckpoint = logCheckpoint==null?null:logCheckpoint.clone();
		}	
	}

	
	
	
	
	
	// private static final long TEST_COUNT = 50000L  ;
	// private static final long TEST_COUNT = 1L  ;
	private static final long TEST_COUNT = 50000L  ;
	private static final long TARGET_TPS = 5000L ;
	private static final long TARGET_TPSR = 20000L ;


	public static void main(String[]args){
		
		try {
			BasicConfigurator.configure() ;
		//	(String storeName, String logPath, String keyPath, long ckeckPointPeriod, long logSize, int keySize,
			//		long mapSize, int packInterval, int readThreads, long needleCachedEntries, long bucketCachedBytes, short nodeId)
			
			/* String path = "/Users/david/Documents/NEEDLES";
			if ( args.length > 0) {
				path = args[0];
			}
			System.out.println("Using directory:" + path);
			*/
			DiskMapManager dmm = new DiskMapManager( "TestStore", 
													"/drive_hd1",
													"/drive_ssd/storage_indexes",
													1000, // Synching period
													60000*5, // Checkpointing period
													2000000000L, // Log file size
													100, // Key max size
													2048, // Nb of buffers
													32768, // Nb of entries (buckets) per buffer
													60, // Compact interval
													8, // Read threads
													TEST_COUNT/3*2*140, // NeedleHeaderCachedEntries (1/20 in cache, 1/2 of typical data set)
													TEST_COUNT/3*2*140, // NeedleCachedBytes (3% of Map, weights 120 bytes/entry)
													true, // Use needle average age instead of needle yougest age
													(short) 0,
													(short) 0,
													0,
													new TokenSynchronizer(1),
													new TokenSynchronizer(1),
													null) ;
			// dmm.getBtm().getCheckPoint().copyFrom(new NeedlePointer()) ; // Removes checkpoint
			
			// System.out.println("Start read for "+TEST_COUNT+" pointers") ;
			Date startDate = new Date() ;
			Date lapDate = new Date() ;
			System.out.println("Start read test for "+TEST_COUNT+" pointers") ;
			long counter = 0 ;
			long lapCounter = 0 ;
			startDate = new Date() ;
			lapDate = new Date() ;
			long failCount = 0 ;
			counter = 0 ;
			while ( counter < TEST_COUNT ) {
				String  key = "MYVERYNICELEY"+counter ;
				// System.out.println("key="+key+"...") ;
				List <Versioned<byte[]>> values = dmm.get(key.getBytes("UTF-8")) ;
				if ( values.size() <= 0 ) {
					failCount++ ;
				} else {
					// Gets previous version	
					byte[] value = values.get(0).getValue() ;
					if ( value == null )
						failCount++ ;
					else if ( value[0] != (byte) ((short)counter % 128 ) )
						failCount++ ;
				}
				counter++ ; 
				Date lapDate2 = new Date() ;
				long spent = lapDate2.getTime()-lapDate.getTime() ;
				if ( spent >= 1000 || (counter - lapCounter > TARGET_TPSR) ) { // Check each second or target tps
					if ( spent < 1000 ) { // pause when tps reached
						Thread.sleep(1000-spent) ;
						// System.out.print(".") ;
					} else
						// System.out.print("*") ;
					lapDate = lapDate2 ; // Reset lap time
					lapCounter = counter ; // Reset tps copunter
				}		
			 }		
			long timeSpent = new Date().getTime() - startDate.getTime() ;
			System.out.println("\n\nProcessed reading of "+TEST_COUNT+" pointers \n"+
						"\tTotal time: "+timeSpent/1000+"s\n"+
						"\tThroughput: "+(TEST_COUNT*1000/timeSpent)+" tps\n"+
						"\tBad results: "+failCount) ;
			
			
			
			
			startDate = new Date() ;
			lapDate = new Date() ;
			System.out.println("Start write test for "+TEST_COUNT+" pointers") ;
			counter = 0 ;
			lapCounter = 0 ;

			
			while ( counter < TEST_COUNT ) {
				String key = "MYVERYNICELEY"+counter ;
				// System.out.println("key="+key+"...") ;
				byte[] value = new byte[128000] ;
				value[0] = (byte) ((short)counter % 128 );
				long chaseDurer = new Date().getTime() ;
				List <Versioned<byte[]>> previousValues = dmm.get(key.getBytes("UTF-8")) ;
				long chaseDurer2 = new Date().getTime() ;
				// System.out.println("Get in "+(chaseDurer2 -chaseDurer)+"ms") ;
				chaseDurer = chaseDurer2 ;
				Version newVersion = null ;
				if ( previousValues.size() <= 0 ) {
					newVersion = new VectorClock() ;
				} else {
					// Gets previous version
					newVersion = previousValues.get(0).cloneVersioned().getVersion() ;
				}
				// Increment version before writing
				((VectorClock) newVersion).incrementVersion(dmm.getNodeId(), new Date().getTime()) ;
				dmm.put(key.getBytes("UTF-8"), value, newVersion) ;
				chaseDurer2 = new Date().getTime() ;
				// System.out.println("Put in "+(chaseDurer2 -chaseDurer)+"ms") ;
				// dmm.putValue(key.getBytes("UTF-8"), value) ;
				counter++ ; 
				Date lapDate2 = new Date() ;
				long spent = lapDate2.getTime()-lapDate.getTime() ;
				if ( spent >= 1000 || (counter - lapCounter > TARGET_TPS) ) { // Check each second or target tps
					if ( spent < 1000 ) { // pause when tps reached
						Thread.sleep(1000-spent) ;
						// System.out.print("("+counter+")") ;
					} else
						// System.out.print("["+counter+"]") ;
					lapDate = lapDate2 ; // Reset lap time
					lapCounter = counter ; // Reset tps copunter
				}		
			}
			
			timeSpent = new Date().getTime() - startDate.getTime() ;
			System.out.println("\n\nWriting before cache commit of "+TEST_COUNT+" pointers \n"+
						"\tTotal time: "+timeSpent/1000+"s\n"+
						"\tThroughput: "+(TEST_COUNT*1000/timeSpent)+" tps") ;
			
			
			System.out.println("Start read for "+TEST_COUNT+" pointers") ;
			startDate = new Date() ;
			lapDate = new Date() ;
			failCount = 0 ;
			counter = 0 ;
			while ( counter < TEST_COUNT ) {
				String  key = "MYVERYNICELEY"+counter ;
				// System.out.println("key="+key+"...") ;
				List <Versioned<byte[]>> values = dmm.get(key.getBytes("UTF-8")) ;
				if ( values.size() <= 0 ) {
					failCount++ ;
				} else {
					// Gets previous version	
					byte[] value = values.get(0).getValue() ;
					if ( value == null )
						failCount++ ;
					else if ( value[0] != (byte) ((short)counter % 128 ) )
						failCount++ ;
				}
				counter++ ; 
				Date lapDate2 = new Date() ;
				long spent = lapDate2.getTime()-lapDate.getTime() ;
				if ( spent >= 1000 || (counter - lapCounter > TARGET_TPSR) ) { // Check each second or target tps
					if ( spent < 1000 ) { // pause when tps reached
						Thread.sleep(1000-spent) ;
						// System.out.print(".") ;
					} else
						// System.out.print("*") ;
					lapDate = lapDate2 ; // Reset lap time
					lapCounter = counter ; // Reset tps copunter
				}		
			 }		
			timeSpent = new Date().getTime() - startDate.getTime() ;
			System.out.println("\n\nProcessed reading of "+TEST_COUNT+" pointers \n"+
						"\tTotal time: "+timeSpent/1000+"s\n"+
						"\tThroughput: "+(TEST_COUNT*1000/timeSpent)+" tps\n"+
						"\tBad results: "+failCount) ;
						
			
			dmm.stopService() ;
			System.out.println("Max cycle time = "+ (dmm.getBtm().getMaxCycleTimePass1()+dmm.getBtm().getMaxCycleTimePass2())) ;
						
		} catch ( Throwable th ) {
			th.printStackTrace() ;
		}
		
	}


	public short getPartitionGroup() {
		return partitionGroup;
	}
	
	
	
}
