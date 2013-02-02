/**
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
 * @author David Mencarelli
 */


package com.yobidrive.diskmap.admin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.yobidrive.diskmap.DiskMapConfiguration;
import com.yobidrive.diskmap.DiskMapStore;

/**
 * Set of utility methods
 *
 */
public class DiskMapShell {

	private static final String PROMPT = "> ";
	
	public static void main(String[] args) throws Exception{

		String voldemortHome = "/Users/david/Documents/EASY-CONCERNS/EZCFOLDERS/_VOLDEMORT/";
		String storeName = "configuration";
		
		//Prepare properties for voldemort
		Properties props = new Properties();
		props.put("node.id","0");
		props.put("voldemort.home", voldemortHome);
		
		DiskMapStore dms = null;
		try {
			//simulate voldemort config 
			VoldemortConfig vc = new VoldemortConfig(props);
			DiskMapConfiguration cfg = new DiskMapConfiguration(vc);
			
			StoreDefinition storeDef = new StoreDefinition(storeName, 
						null, null, null, null, null, null, null, 
						0, null, 0, null, 0, null, null, null,null, 
						null, null, null, null, null, null, null, 0);
			
			dms = (DiskMapStore) cfg.getStore(storeDef);
		} catch (Exception ex) {
			System.err.println("Unable to start shell");
			System.exit(0);
		}
	
		//Command input
		BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
		processCommands(dms, inputReader, false);
		
	}
	
	private static void processCommands(DiskMapStore dms, BufferedReader reader, boolean printCommands) throws IOException {
		for(String line = reader.readLine(); line != null; line = reader.readLine()) {
            if(line.trim().equals(""))
                continue;
            if(printCommands)
                System.out.println(line);
            try {
                if(line.toLowerCase().startsWith("get")) {
                	String key = line.substring("get".length()).trim();
                	ByteArray keyArray = new ByteArray(key.getBytes("UTF-8"));
                	
                	List <Versioned<byte[]>> values = dms.get(keyArray,null) ;
                	
                	
                } else if(line.startsWith("help")) {
                    System.out.println("Commands:");
                    System.out.println("get key -- Retrieve the value associated with the key.");
                    System.out.println("help -- Print this message.");
                    System.out.println("exit -- Exit from this shell.");
                    System.out.println();
                } else if(line.startsWith("quit") || line.startsWith("exit")) {
                    System.out.println("k k thx bye.");
                    System.exit(0);
                } else {
                    System.err.println("Invalid command.");
                }
            } catch(Exception e) {
                System.err.println("Unexpected error:");
                e.printStackTrace(System.err);
            }
            System.out.print(PROMPT);
        }
		
	}
	
	private static final String HEX_DIGITS = "0123456789abcdef";
	public static String toHexString(byte[] v) {
        StringBuffer sb = new StringBuffer(v.length * 2);
        for (int i = 0; i < v.length; i++) {
            int b = v[i] & 0xFF;
             sb.append(HEX_DIGITS.charAt(b >>> 4))
               .append(HEX_DIGITS.charAt(b & 0xF));
        }
        return sb.toString();
    }
	
}
