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


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class DirtyLock {
	private BlockingQueue<Byte> tokenQ = null ;
	
	private int maxOperations = 6 ;
	
	
	public DirtyLock(int maxOps) throws BucketTableManagerException {
		try {
			maxOperations = maxOps ;
			tokenQ = new ArrayBlockingQueue<Byte>(maxOperations) ;
	    
	        for ( int i = 0 ; i < maxOperations ; i++ ) {
	        	tokenQ.put(new Byte((byte)0)) ;
	        }
		} catch ( Throwable th ) {
			throw new BucketTableManagerException("Error building dirty lock ", th) ;
		}
	}
	
	public boolean getOperationToken()  {
		try {
			Byte dummy = tokenQ.take() ;
			return true ;
		} catch ( InterruptedException ie ) {
			return false ;
		}
	}
	
	public boolean putOperationToken() {
		try {
			tokenQ.offer(new Byte((byte)0)) ;
			return true ;
		} catch ( Throwable th ) {
			return false ;
		}
	}
	
	public boolean getCommitToken() {
		for ( int i = 0 ; i < maxOperations; i++ ) {
			if ( !getOperationToken() ) {
				// Give back tokens
				for ( int j = 0 ; j < i; j++ ) {
					if ( !putOperationToken() )
						return false ;
				}
				return false ;
			}
		}
		return true ;
	}
		

	public boolean putCommitToken()  {
		for ( int i = 0 ; i < maxOperations; i++ ) {
			if ( !putOperationToken() )
				return false ;
		}
		
		return true ;
	}
	
	
}
