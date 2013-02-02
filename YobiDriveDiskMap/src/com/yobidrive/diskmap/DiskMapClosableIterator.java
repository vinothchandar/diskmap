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


import com.yobidrive.diskmap.needles.NeedlePointer;
import com.yobidrive.diskmap.needles.NeedleHeader;

import voldemort.VoldemortException;


public class DiskMapClosableIterator {
	long currentBucket = -1 ;
	ArrayList <NeedleHeader> currentNeedleChain = new ArrayList <NeedleHeader>() ;
	int currentEntryInNeedleChain ;
	NeedlePointer nextNeedlePointer = null ;
	DiskMapManager diskMapManager = null ;
	
	
	public DiskMapClosableIterator(DiskMapManager diskMapManager) throws VoldemortException {
		this.diskMapManager = diskMapManager ;
		currentBucket = -1 ;
		currentEntryInNeedleChain = - 1 ;
		prepareNext() ;
	}
	
	protected void prepareNext() throws VoldemortException {
		while ( true ) {
			currentEntryInNeedleChain++ ;
			if ( currentEntryInNeedleChain < currentNeedleChain.size() 
					&& !currentNeedleChain.get(currentEntryInNeedleChain).isDeleted() 
					  && !currentNeedleChain.get(currentEntryInNeedleChain).isNull() )
				return ;
			currentBucket++ ;
			currentNeedleChain.clear() ;
			currentEntryInNeedleChain = -1 ;
			if ( currentBucket >= diskMapManager.getBtm().getMapSize() ) {
				// End of map
				return ;
			}
			// Get pointer 
			NeedlePointer needlePointer = diskMapManager.getBtm().getNeedlePointer(currentBucket) ;
			if ( needlePointer != null && !needlePointer.isEmpty() ) {
				currentNeedleChain = diskMapManager.getNeedleManager().getFullNeedleHeaderChain(needlePointer) ;
				if ( currentNeedleChain == null )
					currentNeedleChain = new ArrayList <NeedleHeader>() ;
			}
		}
	}
	
	public void close() {
		currentEntryInNeedleChain = - 1 ;
		prepareNext() ;

	}

	public boolean hasNext() {
		return currentEntryInNeedleChain >= 0 ;
	}

	public void remove() {
		// TODO Do something...
		// Do nothing
	}

}
