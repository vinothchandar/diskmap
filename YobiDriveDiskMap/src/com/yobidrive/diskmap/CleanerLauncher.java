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


import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yobidrive.diskmap.needles.NeedleLogInfo;



public class CleanerLauncher implements Runnable {
	private static Log logger = LogFactory.getLog(CleanerLauncher.class);
	private DiskMapManager diskMapManager = null ;
	private Thread threadToStart = null ;
	private float highThreshold = 0.80F ;
	// private float lowThreshold = 0.15F ;
	
	
	
	public void startCleaner() throws CleanerException 
    {
		if (threadToStart != null && threadToStart.isAlive() )
			throw new CleanerException("Attempt to start cleaning when already running") ;
		
		threadToStart = new Thread(this) ;
		
		threadToStart.setPriority(1) ;
		threadToStart.start();
    } 
	
	public boolean isCleanerRunning() {
		return threadToStart==null?false:threadToStart.isAlive() ;
	}
	
	public void waitForCleanerStop() {
		if ( !isCleanerRunning() )
			return ;
		try {
			threadToStart.join() ;
		} catch ( InterruptedException ie ) {
			logger.error("Interrupted while waiting for cleaner to stop.") ;
		}
	}
	
	public CleanerLauncher(DiskMapManager diskMapManager) {
		this.diskMapManager = diskMapManager ;
	}

	
	/**
	 * Refresh all concerns in list
	 * Stops 30s or waits for signal (new entry to forward)
	 * and restarts
	 */
	
	public void run() throws CleanerException {
			try {
				clean() ;
			} catch ( Throwable th) {
				logger.fatal("Error during cleaning", th) ;
			}
	}
	
	private void clean() throws CleanerException {
		// 1) get the logInfoList sorted by increasing benefit/cost
		ArrayList<NeedleLogInfo> logInfos = diskMapManager.getBtm().getSortedLogInfo() ;
		// logger.warn("Start cleaning task") ;
		if ( logInfos.size() <= 0 )
			return ;
		// logger.warn("Start cleaning task") ;
		int index = 0 ;
		NeedleLogInfo nli = logInfos.get(index) ;
		while ( nli != null &&  
				( nli.getUtilization() > highThreshold 
				|| nli.getNeedleFileNumber() >= diskMapManager.getBtm().getCheckPoint().getNeedleFileNumber() 
				|| !diskMapManager.cleanLogFile(nli) )
			  ) {
			// logger.info("log file not ready for cleaning "+NeedlePointer.getFormattedNeedleFileNumber(nli.getNeedleFileNumber())+": ratio= "+nli.getBenefitOnCost()+", filled at "+nli.getUtilization()*100+"%") ;
			index++ ;
			if ( index < logInfos.size() )
				nli = logInfos.get(index) ;
			else
				nli = null ;
		}
		// logger.warn("End cleaning task") ;
	}
	
	

}
