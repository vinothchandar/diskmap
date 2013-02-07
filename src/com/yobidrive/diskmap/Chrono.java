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
 */

package com.yobidrive.diskmap;


public class Chrono {
	long start = 0L ;
	long lap = 0L ;
	
	public Chrono() {
		reset() ;
	}
	
	public void reset() {
		/*
		start = new Date().getTime() ;
		lap = start ;
		*/
	}
	
	public void lap(String message, long min) {
		/*
		long newLap = new Date().getTime() ;
		long lapTime = newLap - lap ;
		lap = newLap ;
		if ( lapTime < min )
			return ;
		Logger.getLogger(Chrono.class).info("lap:	"+message+" "+lapTime) ;
		*/
	}
	
	public void total(String message, long min) {
		/*
		long newLap = new Date().getTime() ;
		long lapTime = newLap - start ;
		lap = newLap ;
		return ; 
		if ( lapTime < min )
			return ;
		Logger.getLogger(Chrono.class).info("total:	"+message+" "+lapTime) ;
		*/
	}
}
