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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.TimerTask;



/**
 * pack thread, will purge overflow and deleted block, and move current active data to different channel
 * the trigger conditions could be reached by the overflow size > trigger size
 * or user could issue pack command
 * only user call pack() to activate it
 */

public class PackThread  extends TimerTask {
    private static Log logger = LogFactory.getLog(PackThread.class);

    private DiskMapStore store;
    // force pack data it is triggered by user
    private boolean force;
    private int threshold;

    public PackThread(DiskMapStore store, boolean force) {
        this.store = store;
        this.force = force;
    }

   

    public void run() {
    	/*
        if ( ! force ) {
            if ( store.getOverflow().get() < store.getTrigger()) {
                logger.info("overflow "+ store.getOverflow()+" is below trigger "+ store.getTrigger() );// skip
                return;
            }
            else {
                if ( store.getStatList() != null)
                    store.getStatList().set( 0, System.currentTimeMillis());
            }
        }
        */
   
    }




}

