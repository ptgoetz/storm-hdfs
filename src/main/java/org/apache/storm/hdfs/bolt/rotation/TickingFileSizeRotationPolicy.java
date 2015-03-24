/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.bolt.rotation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

public class TickingFileSizeRotationPolicy implements TickingFileRotationPolicy {
	public static enum TimeUnit {

        SECONDS((long)1000),
        MINUTES((long)1000*60),
        HOURS((long)1000*60*60),
        DAYS((long)1000*60*60*24);

        private long milliSeconds;

        private TimeUnit(long milliSeconds){
            this.milliSeconds = milliSeconds;
        }

        public long getMilliSeconds(){
            return milliSeconds;
        }
    }
	
	public static enum Units {

        KB((long)Math.pow(2, 10)),
        MB((long)Math.pow(2, 20)),
        GB((long)Math.pow(2, 30)),
        TB((long)Math.pow(2, 40));

        private long byteCount;

        private Units(long byteCount){
            this.byteCount = byteCount;
        }

        public long getByteCount(){
            return byteCount;
        }
    }
	
	private static final Logger LOG = LoggerFactory.getLogger(TickingFileSizeRotationPolicy.class);
    
    // File size 
    private long maxBytes;
    private long lastOffset = 0;
    private long currentBytesWritten = 0;
    
    // Time limit
    private long lastTimeWritten = System.currentTimeMillis();
    private long timeLimit = 0;
    private boolean ticking = false;
    
    public TickingFileSizeRotationPolicy(float count, Units units){
        this.maxBytes = (long)(count * units.getByteCount());
    }

    public TickingFileSizeRotationPolicy withTimeLimit(float count, TimeUnit units){
        this.ticking = true;
    	this.timeLimit = (long)(count * units.getMilliSeconds());
        return this;
    }
    
    @Override
    public boolean mark(Tuple tuple, long offset) {
        long diff = offset - this.lastOffset;
        this.currentBytesWritten += diff;
        this.lastOffset = offset;
        if(ticking){
        	this.lastTimeWritten = System.currentTimeMillis();
        }
        
        return this.currentBytesWritten >= this.maxBytes;
    }

    @Override
    public void reset() {
        this.currentBytesWritten = 0;
        this.lastOffset = 0;
        if(ticking){
        	this.lastTimeWritten = System.currentTimeMillis();	
        }
    }

	@Override
	public boolean shouldFinalize() {
		if(ticking){
			if((System.currentTimeMillis() - this.lastTimeWritten) >= this.timeLimit){
				if(this.currentBytesWritten != 0){
					return true;	
				}
			}
		}
		
		return false;
	}

	@Override
	public long getInterval() {
		return this.timeLimit/2;
	}
	
	public static void main(String[] args) throws InterruptedException{
		long t1 =System.currentTimeMillis();
		Thread.sleep(1000);
		long t2 =System.currentTimeMillis();
		System.out.println((t2-t1)*1.0f);
	}
}
