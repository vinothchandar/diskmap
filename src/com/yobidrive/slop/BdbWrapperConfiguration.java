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

package com.yobidrive.slop;

import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.utils.ByteArray;

public class BdbWrapperConfiguration extends BdbStorageConfiguration implements StorageConfiguration {

	public BdbWrapperConfiguration(VoldemortConfig config) {
		super(config) ;
	}
	
	
	public void close() {
		super.close() ;
	}

	public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef, RoutingStrategy strategy) {
		// TODO Auto-generated method stub
		return super.getStore(storeDef, strategy);
	}

	public String getType() {
		return "bdbwrapper";
	}
	
	public String getStats(String storeName, boolean fast) {
        return super.getStats(storeName, fast) ;
    }
	
	public String getEnvStatsAsString(String storeName) throws Exception {
        return super.getEnvStatsAsString(storeName);
    }

    public String getEnvStatsAsString(String storeName, boolean fast) throws Exception {
       return super.getEnvStatsAsString(storeName, fast) ;
    }


    public void cleanLogs() {
        super.cleanLogs() ;
    }

}
