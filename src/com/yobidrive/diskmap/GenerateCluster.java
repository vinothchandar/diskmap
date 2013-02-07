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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;



public class GenerateCluster {

    /** WARNING: do not use for standard Voldemort routing !!!!!!!
     * @param args
     */
    
    static String HEADER = "<cluster>"+
	    "<name>prodcluster</name>"+
	    "<zone>"+
	    "<zone-id>0</zone-id>"+
	     " <proximity-list>1, 2</proximity-list>"+
	    "</zone>"+
	    "<zone>"+
	    "  <zone-id>1</zone-id>"+
	    "  <proximity-list>0, 2</proximity-list>"+
	    "</zone>"+
	    "<zone>"+
	    "  <zone-id>2</zone-id>"+
	    "  <proximity-list>0, 1</proximity-list>"+
	    "</zone>" ;
    
    static String FOOTER = "</cluster>" ;
    
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	
	
	String cs = HEADER ;
	for ( int i = 0 ; i < 3 ; i++ ) {
	    cs = cs+"<server><id>"+i+"</id><host>172.20.3."+(i+11)+"</host><http-port>8081</http-port><socket-port>6666</socket-port><admin-port>6667</admin-port><partitions>" ;
	    for ( int j = 0 ; j < DiskMapStore.NB_MASTER_PARTITIONS; j++ ) {
		if ( j == 0 )
		    cs = cs + ((j*3)+i) ;
		else
		    cs = cs+", "+ ((j*3)+i) ;
	    }
	    cs = cs+"</partitions><zone-id>"+i+"</zone-id></server>" ;
	}
	cs = cs + FOOTER ;
	writeStringToFile(cs, new File("/Users/Francois/Documents/cluster.xml")) ;
	
    }
    
    public static void writeStringToFile(String toWrite, File file) {
	try {
		FileOutputStream fos = new FileOutputStream(file) ;
		OutputStreamWriter  osw = new OutputStreamWriter(fos,"UTF-8") ;
		osw.write(toWrite) ;
		osw.flush() ;
		osw.close() ;
		fos.flush() ;
		fos.close() ;
	} catch ( Throwable th ) {
		th.printStackTrace() ;
	}
}

}
