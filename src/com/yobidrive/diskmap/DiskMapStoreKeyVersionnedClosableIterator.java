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




import voldemort.VoldemortException;
import voldemort.store.PersistenceFailureException;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class DiskMapStoreKeyVersionnedClosableIterator  implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>>  {

		
		private DiskMapStore diskMapStore ;
		private short currentDiskMapManager = 0 ;
		private DiskMapKeyVersionnedClosableIterator currentManagerIterator ;
		
		public DiskMapStoreKeyVersionnedClosableIterator(DiskMapStore diskMapStore) throws VoldemortException {
			this.diskMapStore = diskMapStore ;
			currentDiskMapManager = -1 ;
			openNextIterator() ;
		}
		
		private boolean openNextIterator() {
			currentDiskMapManager += 1 ;
			if ( currentDiskMapManager >= diskMapStore.getNbZones() )
				return false ;
			currentManagerIterator = new DiskMapKeyVersionnedClosableIterator(diskMapStore.getDiskMapManager(currentDiskMapManager)) ;
			return true ;
		}


		public Pair<ByteArray, Versioned<byte[]>> next() {
			if ( currentManagerIterator.hasNext() )
				return currentManagerIterator.next() ; // new entry in current diskMapManager
			if ( !openNextIterator() )
	                throw new PersistenceFailureException("Next called on iterator, but no more items available!");
			return currentManagerIterator.next() ; // are there new entries in the next diskmapManager ?
		}

		public void close() {
			currentManagerIterator.close() ;
			currentManagerIterator = null ;
			currentDiskMapManager = -1 ;
		}

		public boolean hasNext() {
			if ( currentManagerIterator.hasNext() )
				return true ; // new entry in current diskMapManager
			if ( !openNextIterator() )
				return false ; // no other diskMapManager
			return currentManagerIterator.hasNext() ; // are there new entries in the next diskmapManager ?
		}

		public void remove() {
			// TODO Do something...
			// Do nothing
		}
}