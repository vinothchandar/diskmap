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





import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yobidrive.diskmap.needles.NeedlePointer;

/**
 * @author François Vaille
 *
 */
public class BucketShuttleLauncher implements Runnable {
	
	private static Log logger = LogFactory.getLog(BucketTableManager.class);
	private BucketTableManager btm = null ;
	Thread threadToStart = null ;
	NeedlePointer checkPoint = null ;
	
	
	
	
	public void startShuttle(NeedlePointer checkPoint) throws BucketTableManagerException 
    {
		if ( checkPoint==null || checkPoint.isEmpty() ) 
			throw new BucketTableManagerException("start commit shuttle with no checkpoint") ;
		if (threadToStart != null && threadToStart.isAlive() )
			throw new BucketTableManagerException("Attempt to start shuttle when already running") ;
		this.checkPoint = checkPoint ;
		threadToStart = new Thread(this) ;
		//int priority = threadToStart.getPriority() ;
		//if ( priority > Thread.MIN_PRIORITY )
		// 	priority-- ;
		threadToStart.setPriority(2) ;
		threadToStart.start();
    } 
	
	public boolean isShuttleRunning() {
		return threadToStart==null?false:threadToStart.isAlive() ;
	}
	
	public void waitForShuttleStop() {
		if ( !isShuttleRunning() )
			return ;
		try {
			threadToStart.join() ;
		} catch ( InterruptedException ie ) {
			logger.error("Interrupted while waiting for shuttle to stop.") ;
		}
	}
	
	public BucketShuttleLauncher(BucketTableManager bucketTableManager) {
		this.btm = bucketTableManager ;
	}

	
	/**
	 * Refresh all concerns in list
	 * Stops 30s or waits for signal (new entry to forward)
	 * and restarts
	 */
	
	public void run() {
			try {
				btm.forceCommit(checkPoint) ;
			} catch ( Throwable th) {
				logger.fatal("Error during bucket table commit", th) ;
			}
	}
	
	

}
