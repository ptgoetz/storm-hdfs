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
package org.apache.storm.hdfs.bolt.format.partition;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class DefaultPartitionedFileNameFormat implements PartitionedFileNameFormat {
    private String componentId;
    private int taskId;
	private String path = "/tmp/data";
	private String prefix = "";
	private String extension = ".txt";
	private Map<String[], ValueTransformer> partitionKeys = new LinkedHashMap<>(); // Holds partition keys
	private String partitionKeyDelimiter = "/";

	/**
	 * Overrides the default prefix.
	 *
	 * @param prefix
	 * @return
	 */
	public DefaultPartitionedFileNameFormat withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	/**
	 * Overrides the default file extension.
	 *
	 * @param extension
	 * @return
	 */
	public DefaultPartitionedFileNameFormat withExtension(String extension) {
		this.extension = extension;
		return this;
	}

	public DefaultPartitionedFileNameFormat withPath(String path) {
		this.path = path;
		return this;
	}

	public DefaultPartitionedFileNameFormat withPartitionKeyDelimiter(String partitionKeyDelimiter) {
		this.partitionKeyDelimiter = partitionKeyDelimiter;
		return this;
	}
	
	public DefaultPartitionedFileNameFormat initializePartitionWith(Tuple input){
		for (String[] partitionInfo : partitionKeys.keySet()) {
			if(input.contains(partitionInfo[0])){
				partitionInfo[1] = input.getStringByField(partitionInfo[0]);
			}
		}
		
		return this;
	}

	@Override
	public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
	}

	@Override
	public String getName(long rotation, long timeStamp) {
		return this.prefix + "-" + this.componentId + "-" + this.taskId + "_" + timeStamp + "_" + rotation + this.extension;
	}

	@Override
	public String getPath() {
		return this.path;
	}

	@Override
	public String getFullPath() {
		return new Path(this.path, this.getPartition()).toString();
	}

	@Override
	public String getPartition() {
		StringBuilder partitonKeyRepresentation = new StringBuilder("");
		for (String[] partitionInfo : partitionKeys.keySet()) {
			if (partitonKeyRepresentation.length() != 0) {
				partitonKeyRepresentation.append(this.partitionKeyDelimiter);
			}
			
			ValueTransformer transformer = this.partitionKeys.get(partitionInfo);
			if(transformer != null){
				partitonKeyRepresentation.append(transformer.transform(partitionInfo[1]));	
			}
		}

		return partitonKeyRepresentation.toString();
	}

	@Override
	public PartitionedFileNameFormat addPartitionKey(String partitionKey, ValueTransformer valueTransformer) {
		this.partitionKeys.put(new String[]{partitionKey, null}, valueTransformer);
		return this;
	}

}
