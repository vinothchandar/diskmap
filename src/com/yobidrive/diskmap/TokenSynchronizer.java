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


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;




/**
 * A class for restricting concurrent processings. Adapt content to your needs!
 *
 */
public class TokenSynchronizer {
    private static Log logger = LogFactory.getLog(TokenSynchronizer.class);

    private BlockingQueue<Integer> processingQ = null ;

    public TokenSynchronizer(int allowedConcurrent) {
	super();
	processingQ = new ArrayBlockingQueue<Integer>(allowedConcurrent, true) ;
	try {
        	for ( int i = 0 ; i < allowedConcurrent ; i++ ) {
            	 	Integer iInt = new Integer(i) ;
            	 	processingQ.put(iInt) ;
        	}
	} catch (Throwable th) {
            logger.error("Error putting tokens", th) ;
        }
    }
    
    public Integer getToken() {
	/*try {
	    Integer iInt = processingQ.take() ;
	    return iInt ;
	} catch ( Throwable th ) {
		logger.error("Error getting processing token ", th) ;
		throw new DiskMapManagerException("Error getting processing token")	;
	}*/
	return new Integer(1) ;
	
    }

    public void putToken(Integer token) {
	/*try {
	    processingQ.put(token) ;
	} catch ( Throwable th ) {
		logger.error("Error putting back processing token "+token.intValue(), th) ;
		throw new DiskMapManagerException("Error putting processing token")	;
	}
	*/
    }
    
}
