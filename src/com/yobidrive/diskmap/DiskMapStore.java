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
 * @author Francois Vaille
 * 
 * Stat code from Voldemort Copyright 2008-2009 Linked, In
 */

package com.yobidrive.diskmap;


import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;




import voldemort.annotations.jmx.JmxGetter;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Created by EZC Group
 * @author Francois
 *
 * @param <K>
 * @param <V>
 * @param <T>
 * 
 * Some inner classes inspired by third party works under Apache 2 License: cacheStore
 * Created by IntelliJ IDEA.
 * User: mhsieh
 * Date: 1/26/11
 * Time: 9:37 AM
 * 
 */
public class DiskMapStore  implements StorageEngine<ByteArray, byte[], byte[]> {
    public static int NB_MASTER_PARTITIONS = 8 * 1024  ;
    public static int NB_PARTITIONS = NB_MASTER_PARTITIONS * 3 ;
	
    private String storeName;	//For voldermort interface
    long checkPointPeriod ;		// Nb of milliseconds between chekpoints, or 0 for immediate write
    long syncPeriod ;			// Nb of milliseconds between log synching, or 0 for immediate write
    long logSize ; 				// Size of one log file. After reaching this threshold a new file is created
    int keySize ; 				// Nb of bytes for the key. This will be the maximum accepted size
    int nbBuffers ;				
    int entriesPerBuffer ;
    int packInterval; 			// Packing interval
    String logPath[] ; 			// Data Files directory path
    String keyPath ; 			// Key File dir path
    private short nodeId; 		// node id of storage
    private int logNumber = 0 ;	// Current write log file
    private int readThreads = 8;// Needle reader threads / Buckets reader threads
    private List<Long> statList = new CopyOnWriteArrayList<Long>();
    private long deletedRecords = 0L ;
    private long activeRecords = 0L ;
    private long cacheHits = 0L ;
    private long cacheMisses = 0L ;
    private static Log logger = LogFactory.getLog(DiskMapStore.class);
    private TimerTask packTask = null ;
    private RecentCacheStats recentCacheStats ;
    private long needleCachedBytes = 0 ;
    private long needleHeaderCachedBytes = 0 ;
    private boolean useAverage = true ;
    private short nbZones = 1 ;
    private HashFunction partitionHash ;
    private int cachePreload = 0 ;
    private TokenSynchronizer syncsSynchronizer = null ;
    private TokenSynchronizer cleansSynchronizer = null ;
    

    DiskMapManager diskMapManager[] = null ;
    
    
	public DiskMapStore(String storeName, String logPaths, String keyPath, long syncPeriod, long checkPointPeriod, long logSize, int keySize,
						int nbBuffers, int entriesPerBuffer, int packInterval, int readThreads, long needleHeaderCachedBytes, long needleCachedBytes, 
						boolean useAverage, short nodeId, int cachePreload, int concurrentSyncs, int concurrentCleans) {
		this.storeName = storeName;
		this.logPath = logPaths.replace(";",",").replace(" ","").split("\\,");
		this.nbZones = (short) logPath.length ;
		this.keyPath = keyPath ;
		this.logSize = logSize ;
		this.keySize = keySize ;
		this.nbBuffers = nbBuffers ;
		this.entriesPerBuffer = entriesPerBuffer ;
		this.readThreads = readThreads ;
		this.packInterval = packInterval ;
		this.needleCachedBytes = needleCachedBytes ;
		this.needleHeaderCachedBytes = needleHeaderCachedBytes ;
		this.nodeId = nodeId;
		this.syncPeriod = syncPeriod ;
		this.checkPointPeriod = checkPointPeriod ;
		this.useAverage = useAverage ;
		this.partitionHash = new FnvHashFunction() ;
		this.cachePreload = cachePreload ;
		this.syncsSynchronizer = new TokenSynchronizer(concurrentSyncs) ;
		this.cleansSynchronizer = new TokenSynchronizer(concurrentCleans) ;
        
        for ( int i = 0; i < 6 ; i++) statList.add(0L);
        recentCacheStats = new RecentCacheStats(this);
        // hook shutdown process to close all file and flush all delay writes
        // Runtime.getRuntime().addShutdownHook( new Thread( new Shutdown()));
    }
	
	public void initialize()  throws VoldemortException {
	    	logger.info("Starting diskmap for V 0.96...") ;
		diskMapManager = new DiskMapManager[nbZones] ;
		logger.info("Creating one DiskMapManager per drive") ;
		for ( int i = 0 ; i < nbZones ; i++ ) {
		    	logger.info("Creating DiskMapManager for drive "+((logPath.length>i)?logPath[i]:logPath[logPath.length-1])+"...") ;
			diskMapManager[i] = new DiskMapManager(storeName, ((logPath.length>i)?logPath[i]:logPath[logPath.length-1]), keyPath, syncPeriod, checkPointPeriod, logSize, keySize,
				nbBuffers, entriesPerBuffer, packInterval, readThreads, needleHeaderCachedBytes, needleCachedBytes, useAverage, nodeId, (short) i, cachePreload,
				syncsSynchronizer, cleansSynchronizer, this) ;
		}
		logger.info("Finalize initialization of DiskMapManagers...") ;
		for ( int i = 0 ; i < nbZones ; i++ )
		    diskMapManager[i].finalizeInit() ;
	}
	
	
	public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
		// return new DiskMapKeyVersionnedClosableIterator(diskMapManager);
		return new DiskMapStoreKeyVersionnedClosableIterator(this) ;
	}

	public boolean isPartitionAware() {
		return false;
	}

	public ClosableIterator<ByteArray> keys() {
		// return new DiskMapKeyClosableIterator(diskMapManager);
		return new DiskMapStoreKeyClosableIterator(this) ;
	}

	public void truncate() {
		// TODO Auto-generated method stub

	}

	public void close() throws VoldemortException {
		for ( int i = 0 ; i < nbZones; i++ ) {
		if ( diskMapManager[i] !=  null )
			diskMapManager[i].stopService() ;
		}
	}

	public boolean delete(ByteArray key, Version version) throws VoldemortException {
		try {
			return diskMapManager[getZone(key)].delete(key.get(), version) ;
		} catch ( DiskMapManagerException de ) {
			throw new VoldemortException("Error deleting "+key.toString()+", version "+(version==null?null:version.toString()), de) ;
		}
	}
	
	public boolean deleteNoVersionCheck(ByteArray key , Version version) throws DiskMapManagerException {
		return diskMapManager[getZone(key)].deleteNoVersionCheck(key.get(), version) ;
	}


	public  List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
		try {
			return diskMapManager[getZone(key)].get(key.get()) ;
		} catch ( DiskMapManagerException de ) {
			throw new VoldemortException("Error getting "+key.toString(), de) ;
		}
	}

	
	public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys, Map<ByteArray, byte[]> transforms) throws VoldemortException {
		try {
			Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
	        for(ByteArray key: keys) {
	            List<Versioned<byte[]>> found = diskMapManager[getZone(key)].get(key.get()) ;
	            if(found.size() > 0)
	                result.put(key, found);
	        }
	        return result;
        } catch ( DiskMapManagerException de ) {
			throw new VoldemortException("Error getting keys", de) ;
		}
	}


	public Object getCapability(StoreCapabilityType arg0) {
		throw new NoSuchCapabilityException(arg0, getName());
	}


	public String getName() {
		return storeName;
	}


	public List<Version> getVersions(ByteArray key) {
		try {
			return diskMapManager[getZone(key)].getVersions(key.get()) ;
		} catch ( DiskMapManagerException de ) {
			throw new VoldemortException("Error getting versions for "+key.toString(), de) ;
		}
	}


	public void put(ByteArray key, Versioned <byte[]> value, byte[] transform) throws VoldemortException {
		try {
			if ( !diskMapManager[getZone(key)].put(key.get(), value.getValue(), value.getVersion())) {
				throw new ObsoleteVersionException("Error putting "+key.toString()+", version "+(value.getValue()==null?null:value.getValue().toString())+" is obsolete");
			}
		} catch ( DiskMapManagerException de ) {
			throw new VoldemortException("Error putting "+key.toString()+", version "+(value.getValue()==null?null:value.getValue().toString()), de) ;
		}
	}
	
	public void putNoVersionCheck(ByteArray key, byte[] value, VectorClock clock) throws DiskMapManagerException {
		diskMapManager[getZone(key)].putNoVersionCheck(key.get(), value, clock) ;
	}
	
	public short getZone(ByteArray key) throws VoldemortException {
		if ( nbZones <= 1 )
			return 0 ;
		
		int index = abs(partitionHash.hash(key.get())) ;
		
		return (short) (index % nbZones) ;
	}
	
	private static int abs(int a) {
	        if(a >= 0)
	            return a;
	        else if(a != Integer.MIN_VALUE)
	            return -a;
	        return Integer.MAX_VALUE;
	}

   
    

    public void startPackThread(){
    	// check pack data damon thread  for each hour
        if ( packInterval > 0) {
            logger.info("Start the pack data thread");
            packTask = new PackThread  (this, false);
            new Timer().schedule(packTask ,  packInterval*1000, packInterval*1000 );
        }
    }
	
	class RecentCacheStats{

        private final DiskMapStore diskMapStore;
        private Long millisecondsPerInterval = 5L;
        private Timer timer = new Timer();

        private long recentCacheHits;
        private long  recentCacheMisses;

        private long cumulativeCacheHits;
        private long cumulativeCacheMisses;

        // add writeThread
        private long recentSkips;
        private long recentEmpty;
        private long recentCount;
        // private long cumSkips;
        // private long cumEmpty;
        // private long cumCount;

        public long getRecentCacheHits(){ return recentCacheHits; }
        public long getRecentCacheMisses(){ return recentCacheMisses; }

        public long getRecentSkips() {
            return recentSkips;
        }

        public long getRecentEmpty() {
            return recentEmpty;
        }

        public long getRecentCount() {
            return recentCount;
        }

        public RecentCacheStats(DiskMapStore  store){
        	this.diskMapStore = store;

            timer.schedule(
                         new TimerTask(){
                             public void run(){
                                 long currCacheHits = diskMapStore.getCacheHits();
                                 long currCacheMisses = diskMapStore.getCacheMisses();
                                 recentCacheHits = currCacheHits - cumulativeCacheHits;
                                 recentCacheMisses = currCacheMisses - cumulativeCacheMisses;
                                 cumulativeCacheHits = currCacheHits;
                                 cumulativeCacheMisses = currCacheMisses;
                                 /* 
                                  if ( writeBackThread > 0  &&  writeThreadList != null ) {
                                     long skips =0 , empty = 0 , count = 0;
                                     for ( int i = 0 ; i < writeBackThread ; i ++ ) {
                                         skips += writeThreadList.get(i).getDirtyNo() ;
                                         empty += writeThreadList.get(i).getEmptyNo();
                                         count += writeThreadList.get(i).getCount();
                                     }
                                     recentSkips = skips - cumSkips;
                                     recentEmpty = empty - cumEmpty;
                                     recentCount = count - cumCount;

                                     cumSkips = skips;
                                     cumEmpty = empty;
                                     cumCount = count;
                                 } 
                                 */
                             } 
                         },
                         millisecondsPerInterval, 
                         millisecondsPerInterval
                 );
             }

       
    }
	
	private String getCurrentFile() {
		return logPath+"/"+Integer.toHexString(logNumber)+".dml" ;
	}
	
	@JmxGetter(name="Store Name")
    public String getStoreName(){ return storeName; }
    @JmxGetter(name="Node Id")
    public String getNodeId(){ return ""+nodeId; }
    @JmxGetter(name="File Name")
    public String getFileName(){ return getCurrentFile() ; }
    @JmxGetter(name="Total Records")
    public long getTotalRecords(){ return deletedRecords+activeRecords ; }
    @JmxGetter(name="Total Deleted Records")
    public long getTotalDeletedRecords(){ return deletedRecords ; }
    @JmxGetter(name="Total Active Records")
    public long getTotalActiveRecords(){ return activeRecords ; }
    @JmxGetter(name="Total Active Records Percentage")
    public long getTotalActiveRecordsPercentage(){
        long hit = getTotalActiveRecords();
        long miss = getTotalDeletedRecords();
        return ( miss == 0 ? 100 : ( hit * 100 / (hit+miss) )); }
    // @JmxGetter(name="File Size")
    // public long getFileSize(){ return cacheStore.getList().get(cacheStore.getCurIndex()).getDataOffset(); }
    
    @JmxGetter(name="Cache Hits")
    public long getCacheHits(){ return cacheHits ; }
    @JmxGetter(name="Cache Misses")
    public long getCacheMisses(){ return cacheMisses ; }
    @JmxGetter(name="Cache Hit Percentage")
    public long getCacheHitPercentage(){ return ( cacheMisses == 0 ? 100 : ( cacheHits * 100 / (cacheHits+cacheMisses) )); }
    @JmxGetter(name="Recent Cache Hits")
    public long getRecentCacheHits(){ return recentCacheStats.getRecentCacheHits(); }
    @JmxGetter(name="Recent Cache Misses")
    public long getRecentCacheMisses(){ return recentCacheStats.getRecentCacheMisses(); }
    @JmxGetter(name="Recent Cache Hit Percentage")
    public long getRecentCacheHitPercentage(){
        long hit = recentCacheStats.getRecentCacheHits();
        long miss = recentCacheStats.getRecentCacheMisses();
        return ( miss == 0 ? 100 : ( hit * 100 / (hit+miss) )); }    @JmxGetter(name="Get current store info")
    public String getStoreInfo() {
        return "Not implemented" ;
    }

		public DiskMapManager[] getDiskMapManager() {
			return diskMapManager;
		}
		
		public DiskMapManager getDiskMapManager(int i) {
			return diskMapManager[i];
		}

		public short getNbZones() {
			return nbZones;
		}
		
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
			
				
				
				DiskMapStore dms = new DiskMapStore(						"teststore",
														"/drive_hd1;/drive_hd2",
														"/drive_ssd/storage_indexes",
														1000L, // Synching period
														60000*5L, // Checkpointing period
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
														1,
														1,
														1) ;
				// dmm.getBtm().getCheckPoint().copyFrom(new NeedlePointer()) ; // Removes checkpoint
				dms.initialize() ;
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
					List <Versioned<byte[]>> values = dms.get(new ByteArray(key.getBytes("UTF-8")),null) ;
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
					List <Versioned<byte[]>> previousValues = dms.get(new ByteArray(key.getBytes("UTF-8")),null) ;
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
					((VectorClock) newVersion).incrementVersion(0, new Date().getTime()) ;
					Versioned<byte[]> versionned = new Versioned<byte[]>(previousValues.size() <= 0?value:previousValues.get(0).cloneVersioned().getValue(), newVersion) ;
					dms.put(new ByteArray(key.getBytes("UTF-8")), versionned, null) ;
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
					List <Versioned<byte[]>> values = dms.get(new ByteArray(key.getBytes("UTF-8")),null) ;
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
							
				
				dms.close();
				// System.out.println("Max cycle time = "+ (dms.getBtm().getMaxCycleTimePass1()+dmm.getBtm().getMaxCycleTimePass2())) ;
							
			} catch ( Throwable th ) {
				th.printStackTrace() ;
			}
			
		}

		@Override
		public boolean beginBatchModifications() {
			return false;
		}

		@Override
		public boolean endBatchModifications() {
			return false;
		}

		@Override
		public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(
				int partition) {
			return null;
		}

		@Override
		public boolean isPartitionScanSupported() {
			return false;
		}

		@Override
		public ClosableIterator<ByteArray> keys(int partition) {
			return null;
		}
        

}
