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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;




public class DiskMapConfiguration implements StorageConfiguration {
	private static Log logger = LogFactory.getLog(DiskMapConfiguration.class);

    public static final String TYPE_NAME = "diskmap";
    private final ConcurrentMap<String, DiskMapStore> stores = new ConcurrentHashMap<String, DiskMapStore>();
    private final Object lock = new Object();
    public static final String CHECKPOINT_PERIOD ="checkpointperiod.seconds"; // seconds
    public static final String SYNC_PERIOD ="syncperiod.milliseconds"; // milliseconds 
    public static final String LOG_SIZE  = "logsize.mb";
    public static final String NEEDLECACHE  = "needlecache.mb";
    public static final String NEEDLEHEADERCACHE  = "needleheadercache.mb";
    // public static final String NBZONES  = "partition.zones";
    public static final String LOG_PATH  = "log.path";
    public static final String FORCEDNAME  = "forced.name";
    public static final String KEY_PATH  = "key.path";
    public static final String KEY_SIZE  = "key.size";
    public static final String NB_BUFFERS  = "buffers.count";
    public static final String ENTRIES_PER_BUFFER  = "buffers.entries";
    public static final String PACK_INTERVAL = "packinterval.minutes";
    public static final String READ_THREADS="read.threads";
    public static final String CLEAN_USEAVERAGE="aging.useaverage";
    public static final String NEEDLECACHE_PRELOAD ="needlecache.feedonwrite";
    public static final String CONCURRENT_SYNCS ="concurrent.syncs";
    public static final String CONCURRENT_CLEANS ="concurrent.syncs";

   
    private String typeName = null ;
    
    VoldemortConfig voldemortConfig ;

	
	
	
	public DiskMapConfiguration(VoldemortConfig config) {
	        this.voldemortConfig = config;
	        Props props = loadProp(voldemortConfig);
	        typeName = props.getString("diskmap."+FORCEDNAME, TYPE_NAME);
            logger.info(FORCEDNAME+" "+typeName);
	    }
	
	
	
	public void update(StoreDefinition storeDef) {
	    logger.info("Updating storeDef") ;
	}


	public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {
	    String name = storeDef.getName() ;
	DiskMapStore  vs = stores.get(name);
        if ( vs != null ) 
        	return vs;
        // else, create a new store
        synchronized(lock) {
            // double check it again
            
            vs = stores.get(name);
            // create a new one, when it is not existing
            if ( vs == null ) {
                Props props = loadProp(voldemortConfig);
                long syncPeriod = props.getLong("diskmap."+name+"."+SYNC_PERIOD,props.getLong("diskmap."+SYNC_PERIOD, 1000L));
                logger.info("Sync (ms)"+" "+syncPeriod);
                long checkPointPeriod = props.getLong("diskmap."+name+"."+CHECKPOINT_PERIOD,props.getLong("diskmap."+CHECKPOINT_PERIOD, 10000L)) * 1000L ;
                logger.info("Checkpoint (ms)"+" "+checkPointPeriod);
                long logSize = props.getLong("diskmap."+name+"."+LOG_SIZE, props.getLong("diskmap."+LOG_SIZE, 10000L)) * 1000000L; // 10GB by default
                logger.info(LOG_SIZE+" "+logSize);
                int keySize = props.getInt("diskmap."+name+"."+KEY_SIZE, props.getInt("diskmap."+KEY_SIZE, 64)); // 64 Bytes by default
                logger.info(KEY_SIZE+" "+keySize);
                int nbBuffers = props.getInt("diskmap."+name+"."+NB_BUFFERS, props.getInt("diskmap."+NB_BUFFERS,100)); // 100M entries per default
                logger.info(NB_BUFFERS+" "+nbBuffers);
                int entriesPerBuffer = props.getInt("diskmap."+name+"."+ENTRIES_PER_BUFFER, props.getInt("diskmap."+ENTRIES_PER_BUFFER,1000000)); // 100M entries per default
                logger.info(ENTRIES_PER_BUFFER+" "+entriesPerBuffer);
                int packInterval = props.getInt("diskmap."+name+"."+PACK_INTERVAL, props.getInt("diskmap."+PACK_INTERVAL, 60)) * 60; // 1 Hour by default
                logger.info(PACK_INTERVAL+" "+packInterval);
                int readThreads = props.getInt("diskmap."+name+"."+READ_THREADS, props.getInt("diskmap."+READ_THREADS, 8)); // 1 by default
                logger.info(READ_THREADS+" "+readThreads);
                String logPath = props.getString("diskmap."+name+"."+LOG_PATH, props.getString("diskmap."+LOG_PATH, "/opt/diskmapstore"));
                logger.info(LOG_PATH+" "+logPath);
                String keyPath = props.getString("diskmap."+name+"."+KEY_PATH, props.getString("diskmap."+KEY_PATH, "/opt/diskmapstore"));
                logger.info(KEY_PATH+" "+keyPath);
                long needleCachedBytes = props.getLong("diskmap."+name+"."+NEEDLECACHE, props.getLong("diskmap."+NEEDLECACHE, 100L)) ; // 100MB by default
                logger.info(NEEDLECACHE+" "+needleCachedBytes);
                long needleHeaderCachedBytes = props.getLong("diskmap."+name+"."+NEEDLEHEADERCACHE, props.getLong("diskmap."+NEEDLEHEADERCACHE, 100L)) ; // 100MB by default
                logger.info(NEEDLEHEADERCACHE+" "+needleHeaderCachedBytes);
                boolean useAverage = props.getBoolean("diskmap."+name+"."+CLEAN_USEAVERAGE, props.getBoolean("diskmap."+CLEAN_USEAVERAGE, true)) ; // 100MB by default
                logger.info(CLEAN_USEAVERAGE+" "+useAverage);
                int cachePreload = props.getInt("diskmap."+name+"."+NEEDLECACHE_PRELOAD, props.getInt("diskmap."+NEEDLECACHE_PRELOAD, 0)) ; // feed cache on writes
                logger.info(NEEDLECACHE_PRELOAD+" "+cachePreload);
                // int nbZones = props.getInt("diskmap."+name+"."+NBZONES, props.getInt("diskmap."+NBZONES,1)) ; // 1 zone by default. Never change!
                // logger.info(NBZONES+" "+nbZones);
                int concurrentSyncs = props.getInt("diskmap."+name+"."+CONCURRENT_SYNCS, props.getInt("diskmap."+CONCURRENT_SYNCS,1)) ; // 1 zone by default. Never change!
                logger.info(CONCURRENT_SYNCS+" "+concurrentSyncs);
                int concurrentCleans = props.getInt("diskmap."+name+"."+CONCURRENT_CLEANS, props.getInt("diskmap."+CONCURRENT_CLEANS,1)) ; // 1 zone by default. Never change!
                logger.info(CONCURRENT_CLEANS+" "+concurrentCleans);
                

                
             
                vs = new DiskMapStore(name, logPath, keyPath, syncPeriod, checkPointPeriod, logSize, keySize, 
                												 nbBuffers, entriesPerBuffer, packInterval, readThreads, 
                												 needleHeaderCachedBytes*1000000,
                												 needleCachedBytes* 1000000,
                												 useAverage,
                												 (short) voldemortConfig.getNodeId(),
                												 cachePreload,
                												 concurrentSyncs,
                												 concurrentCleans);
                
                
                DiskMapStore previousDiskMapStore= stores.putIfAbsent(name, vs);
                //check it agin
                if ( previousDiskMapStore != null ) 
                	vs = previousDiskMapStore ;
                else {
                	vs.initialize() ;
                }
            }
            return vs;
        } //lock
	}
	
	
	 public static Props loadProp(VoldemortConfig config) {
	        String voldemortHome = config.getVoldemortHome();
	        if(voldemortHome == null)
	            throw new RuntimeException("Missing environment variable "
	                                             + VoldemortConfig.VOLDEMORT_HOME_VAR_NAME
	                                             );

	        String propertiesFile = voldemortHome + File.separator + "config" + File.separator
	                                + "server.properties";
	        Props properties = null;
	        try {
	            properties = new Props(new File(propertiesFile));
	            properties.put("voldemort.home", voldemortHome);
	            return properties;
	        } catch(IOException e) {
	            throw new RuntimeException(e);
	        }
	    }


	public String getType() {
		if ( typeName == null )
			throw new RuntimeException("No type name defined for diskmap store") ;
		return typeName ;
	}
	
	public void cleanLogs() {
	        // Do nothing
	    }

	public void close() {  
        try {
        	Iterator<DiskMapStore> it = stores.values().iterator() ;
        	while ( it.hasNext()) {
        		DiskMapStore  dms = it.next() ;
        		dms.close() ;
        	}
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
	}

}
