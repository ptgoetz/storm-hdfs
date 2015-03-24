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
package org.apache.storm.hdfs.bolt.partition;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.format.partition.PartitionedFileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MultiFSRotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class DefaultExportManager extends AbstractExportManager{

	private static final Logger LOG = LoggerFactory.getLogger(PartitionedHdfsBolt.class);

    private transient FSDataOutputStream out;

	public DefaultExportManager withDistributedFS(FileSystem distributedFS) {
		this.distributedFs = distributedFS;
		return this;
	}
	
	public DefaultExportManager withLocalFS(FileSystem localFS) {
		this.localFs = localFS;
		return this;
	}

	public DefaultExportManager useLocalForWrite() {
		this.writerFs = localFs;
		return this;
	}
	
	public DefaultExportManager useDistributedForWrite() {
		this.writerFs = distributedFs;
		return this;
	}
	
    public DefaultExportManager withFileNameFormat(PartitionedFileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public DefaultExportManager withRecordFormat(RecordFormat format){
        this.recordFormat = format;
        return this;
    }

    public DefaultExportManager withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public DefaultExportManager withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public DefaultExportManager addRotationAction(MultiFSRotationAction action){
        this.rotationActions.add(action);
        return this;
    }
    
    public DefaultExportManager addRotationActions(List<MultiFSRotationAction> actions){
        this.rotationActions.addAll(actions);
        return this;
    }

    @Override
    public void writeToOutputFile(Tuple tuple, OutputCollector collector){
    	try {
            byte[] bytes = this.recordFormat.format(tuple);
            synchronized (this.writeLock) {
                out.write(bytes);
                this.offset += bytes.length;
                this.syncOutputFile();
                if (this.syncPolicy.mark(tuple, this.offset)) {
                	this.syncOutputFile();
                    this.syncPolicy.reset();
                }
            }

            collector.ack(tuple);

            if(this.rotationPolicy.mark(tuple, this.offset)){
                rotateOutputFile(); // synchronized
                this.offset = 0;
                this.rotationPolicy.reset();
            }
        } catch (IOException e) {
            LOG.warn("write/sync failed.", e);
            this.collector.fail(tuple);
        }
    }
    
    @Override
    void closeOutputFile() throws IOException {
        this.out.close();
    }

    @Override
    Path createOutputFile() throws IOException {
    	String partitionKey = this.fileNameFormat.getPartition().toLowerCase();
    	Path destinationPath = new Path(this.fileNameFormat.getPath(), partitionKey);
    	if(!writerFs.exists(destinationPath)){
    		writerFs.mkdirs(destinationPath);
    	}
    	
        Path path = new Path(destinationPath, this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
//        if(this.writerFs instanceof LocalFileSystem){
//        	this.out = ((LocalFileSystem) this.writerFs).getRawFileSystem().create(path);
//        }else{
        this.out = this.writerFs.create(path);
//        }
        return path;
    }

	@Override
	void syncOutputFile() throws IOException {
		if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
	}
	
}
