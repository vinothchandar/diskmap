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



import com.yobidrive.diskmap.needles.Needle;
import com.yobidrive.diskmap.needles.NeedleHeader;

import voldemort.VoldemortException;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class DiskMapKeyVersionnedClosableIterator extends DiskMapClosableIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>>  {
	
	public DiskMapKeyVersionnedClosableIterator(DiskMapManager diskMapManager) throws VoldemortException {
		super(diskMapManager) ;
	}

	public Pair<ByteArray, Versioned<byte[]>> next() {
		if ( currentEntryInNeedleChain < 0 )
			return  null ; // Nothing available (error)
		// Get key, data and version 
		NeedleHeader needleHeader = currentNeedleChain.get(currentEntryInNeedleChain) ;
		Needle needle = diskMapManager.getNeedleManager().getNeedleFromCache(needleHeader.getNeedleAddress()) ;	
		return Pair.create( new ByteArray(needle.getKeyBytes()), new Versioned<byte[]>(needle.getNotNullData(), needle.getVersion()) );
	}

}
