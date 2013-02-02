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




import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;




/**
 * 
 * Consistent hashing function for disk based associative array with load factor potentially > 1
 * Builds a hash for any bucket number (power of 2) that fits in a long
 * 
 * Taken from http://www.isthe.com/chongo/tech/comp/fnv
 * 
 * Implementation created by EZC Group S.A.
 * @author Francois Vaille
 * 
 */
public class BucketFNVHash {
    public static final long FNV_BASIS_32 = 0x811c9dc5L;
    public static final long FNV_PRIME_32 = 0x1000193L;
    public static final long twoPower32 = 0x200000000L ;
    // public static final long MAX_INT = 0x7FFFFFFF;
    // public static final long FNV_BASIS_64 = 0xCBF29CE484222325L;
    // public static final long FNV_PRIME_64 = 0x100000001B3L;
    
    // private long numBuckets = 2 ; // MAX_INT ; 
    private int bitLenght =  1;
    private long mask = 1L ;
    private static final String PREFIX = "YOBI" ;
    byte prefixBytes[] = null ;
    // private long twoPowerN = 2 ;
    
    /** 
     * @param numBuckets Should be a power of 2 < 2 power 31 (4G)
     */
    public BucketFNVHash(long numBuckets) throws BucketTableManagerException {
    	// this.numBuckets = numBuckets ;
    	long result =  2L;
    	for ( int i = 1 ; i < 32 ; i++ ) {
    		if ( result == numBuckets ) {
    			bitLenght = i ;
    			break ;
    		}
    		result = result * 2 ;
    	}
    	if ( bitLenght <= 1 )
    		throw new BucketTableManagerException("Buckets ("+numBuckets+") is not a power of 2") ;
    	// Computes reduction mask
    	mask = (1L<<bitLenght)-1;
    	try {
    		prefixBytes = PREFIX.getBytes("UTF-8") ;
    	} catch ( Throwable th ) {
    		throw new BucketTableManagerException("UTF-8 not supported", th) ;
    	}
    	
    }

    public long hash(byte[] sourceKey) {
    	
    	// First we need to rehash so that the distribution is the full scale instead of the consistent hashing partition
    	byte[] key = new byte[sourceKey.length+prefixBytes.length] ;
    	System.arraycopy(prefixBytes, 0, key, 0, prefixBytes.length) ;
    	System.arraycopy(sourceKey, 0, key, prefixBytes.length, sourceKey.length) ;
    	
        long hash = FNV_BASIS_32 ;
        for(int i = 0; i < key.length; i++) {
            hash ^= (0xFF & key[i]) ;
            hash *= FNV_PRIME_32;
            hash = hash % twoPower32 ;
            // if ( hash < 0 || hash >= numBuckets )
            // 	System.out.println("-") ;
        }
      
        // Check if reduction required
        if ( bitLenght == 32 )
        	return hash ;
        // Reduce using XOR Folding method
        long result1 = (hash>>bitLenght) ;
        ///return (hash>>(32-bitLenght)) & mask ;
        long result2 = (hash & mask);
        /////////return result2 ;
        long result = result1 ^ result2 ;
        return result ;
        // return Math.abs(hash) % numBuckets;
         
    }
    
    private static final long FNV_BASIS = 0x811c9dc5;
    private static final long FNV_PRIME = (1 << 24) + 0x193;
    
    public long hashVoldemort(byte[] key) {
        long hash = FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash ^= 0xFF & key[i];
            hash *= FNV_PRIME;
        }

        return hash;
    }
    
    
    
	
    public long hash(String key) {
    	byte[] bytes ;
    	try {
    		bytes = key.getBytes("UTF-8") ;
    	} catch ( Throwable th ) {
    		return -1 ;
    	}
    	return hash(bytes) ;
    }
    
    public static void main(String[] args) {
    	  // need to pass Ethernet address; can either use real one (shown here)
    	  EthernetAddress nic = EthernetAddress.fromInterface();
    	  // or bogus which would be gotten with: EthernetAddress.constructMulticastAddress()
    	  TimeBasedGenerator uuidGenerator = Generators.timeBasedGenerator(nic);
    	  // also: we don't specify synchronizer, getting an intra-JVM syncer; there is
    	  // also external file-locking-based synchronizer if multiple JVMs run JUG
    	  
    	  
    	  
        int numIterations = 4096 * 16 * 24;
        int numBuckets = 4096 * 16 * 2 ; ;
        int[] buckets = new int[numBuckets];
        BucketFNVHash hash = new BucketFNVHash(numBuckets);
        long smallest = Integer.MAX_VALUE ;
        long biggest = Integer.MIN_VALUE ;
        int collisionsI = 0 ;
        int collisions6 = 0 ;
        int collisions2 = 0 ;
        int collisions3 = 0 ;
        int collisions4 = 0 ;
        int collisions5 = 0 ;
        long smallestI = Integer.MAX_VALUE ;
        long biggestI = Integer.MIN_VALUE ;
        int counter = 0 ;
        int maxCols = 0 ;
        
        for(int i = 0; i < numIterations; i++) {
        	String key = uuidGenerator.generate().toString();
            long valVoldemort = Math.abs(hash.hashVoldemort(key.getBytes())) ;
            if ( valVoldemort < smallest )
            	smallest = valVoldemort ;
            if ( valVoldemort > biggest )
            	biggest = valVoldemort ;
            
            long partition = valVoldemort % 16 ; // 16 partitions
            if ( partition == 7 ) {
            	counter++ ; // 1 more in partition
            	int val = (int) hash.hash(key.getBytes());
            	if ( val < smallestI )
                	smallestI = val ;
                if ( val > biggestI )
                	biggestI = val ;
                buckets[val]++ ;
            	if ( buckets[val] > maxCols )
            		maxCols = buckets[val] ;
                if ( buckets[val] > 1 ) {
                	collisionsI++ ;
                }
                if ( buckets[val] == 2 ) {
                	collisions2++ ;
                }
                if ( buckets[val] == 3 ) {
                	collisions3++ ;
                }
                if ( buckets[val] == 4 ) {
                	collisions4++ ;
                }
                if ( buckets[val] == 5 ) {
                	collisions5++ ;
                }
                if ( buckets[val] > 5 ) {
                	collisions6++ ;
                }
            }
            
        }

        System.out.println("Smallest="+smallest+", Biggest="+biggest+", SmallestI="+smallestI+", BiggestI="+biggestI+"/"+numBuckets+", Partition rate="+((float)counter/numIterations*100)+"% (target 6,25%), Collision rate="+((float)collisionsI*100/counter)+"%, Fill rate"+((float) counter/numBuckets*100)+", Max cols="+(maxCols-1)) ;
        System.out.println("Chains 2:"+((float)collisions2*100/collisionsI)+", 3:"+((float)collisions3*100/collisionsI)+
        		", 4:"+((float)collisions4*100/collisionsI)+
        		", 5:"+((float)collisions5*100/collisionsI)+
        		", 6:"+((float)collisions6*100/collisionsI)) ;
    }
    
    
}
