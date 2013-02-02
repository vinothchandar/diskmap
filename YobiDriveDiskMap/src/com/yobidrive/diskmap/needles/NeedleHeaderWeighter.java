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

import com.google.common.cache.Weigher;

public class NeedleHeaderWeighter implements Weigher <NeedlePointer, NeedleHeader> {

	public int weigh(NeedlePointer pointer, NeedleHeader needle) {
		return NeedlePointer.POINTERSIZE 
				+ (needle.getKeyBytes()==null?0:needle.getKeyBytes().length)
				+ (needle.getVersion()==null?0:needle.getVersion().toBytes().length)
				+ 100 ; // google cache entry overhead
	}
	
	public static int typicalWeight(int keySize) {
		return NeedlePointer.POINTERSIZE 
		+ keySize
		+ 500 
		+ 100 ;
	}

}
