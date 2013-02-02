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

package com.yobidrive.diskmap.recovery;

import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.yobidrive.diskmap.needles.Needle;
import com.yobidrive.diskmap.needles.NeedleManager;

/**
 * Set of utility methods
 * @author david
 *
 */
public class DiskMapRecovery {

	public static void main(String[] args) {
		
		if ( args.length != 2 ) {
			System.out.println("Usage: DiskMapRecovery key pathToYdmFiles");
			System.exit(0);
		}
		
		String key = args[0];
		String path= args[1];
		
		recoverKeyValues(key, path, true);
		
		//recoverKeyValues("IDX:SMARTFOLDERS:USERS", "/Users/david/Desktop/Recovery", true);
	}
	
	/**
	 * List all entries corresponding to the key inside the ydmFile. 
	 * 
	 * @param key
	 * @param ydmSource
	 */
	private static void recoverKeyValues(String key, String logFilePath, boolean dumpValue) {
		System.out.println("Trying to recover key '"+key+"' , YdmFilesPath="+logFilePath);
		NeedleManager needleManager = new NeedleManager(logFilePath, -1, 1, 0, 0, 0);
		needleManager.initialize();
		
		int firstFileNumber = needleManager.getFirstFileNumber();
		int lastFileNumber	= needleManager.getLastFileNumber();
		
		//Look at all files
		boolean found = false;
		for ( int needleFileNumber=firstFileNumber ; needleFileNumber<=lastFileNumber ; needleFileNumber++) {
			Needle needle = needleManager.readFirstNeedleToRecoverFromDisk(needleFileNumber) ;
			while ( needle !=  null ) {
				if (key.equals(needle.getKey())) { //Matched key
					found = true;
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:S") ;
					System.out.println("[Match]\tVersion:"+needle.getVersion().toString()+"\t"+sdf.format(new Date(needle.getVersion().getTimestamp()))+"\tSize="+needle.getSize()+"\tFlag="+needle.getFlags()+"\tnumber="+needle.getNeedleNumber()+"\tprev="+needle.getPreviousNeedle());
					if ( dumpValue ) {
						try {
							sdf = new SimpleDateFormat("yyyyMMdd_HHmmsS") ;
							String dumpFileName = "dump_"+sdf.format(new Date(needle.getVersion().getTimestamp()))+".bin";
							FileOutputStream fos = new FileOutputStream(dumpFileName);
							fos.write(needle.getData());
							fos.flush();
							fos.close();
							System.out.println("Dumped to "+dumpFileName);
						} catch (Exception ex) {
							System.err.println("Error dumping entry");
						}
					}
				}
				needle = needleManager.readNextNeedleToImportFromDisk() ;
			}
		}
		
		if ( !found )
			System.out.println("Sorry, nothing found");
	}
	
}
