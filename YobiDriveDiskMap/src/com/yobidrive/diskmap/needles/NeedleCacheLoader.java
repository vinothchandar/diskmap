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

package com.yobidrive.diskmap.needles;

import com.google.common.cache.CacheLoader;

public class NeedleCacheLoader extends CacheLoader <NeedlePointer, Needle> {

	private NeedleManager needleManager = null ;
	
	@Override
	public Needle load(NeedlePointer needlePointer) throws NeedleManagerException {
		if ( needleManager==null )
			return null;
		return needleManager.getNeedleFromDisk(needlePointer) ;
	}
	
	public NeedleCacheLoader(NeedleManager needleManager) {
		this.needleManager = needleManager ;
	}

}
