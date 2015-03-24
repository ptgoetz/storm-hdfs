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
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;

public abstract class AbstractPartitionedHdfsBolt extends BaseRichBolt {
	protected LRUMap currentFiles = new LRUMap(100);
	protected AbstractExportManager exportManagerPrototype;
	protected OutputCollector collector;
	protected transient FileSystem localFs;
	protected transient FileSystem distributedFs;
	protected String fsUrl;
	protected String configKey;
	protected transient Configuration hdfsConfig;

	/**
	 * Marked as final to prevent override. Subclasses should implement the doPrepare() method.
	 * 
	 * @param conf
	 * @param topologyContext
	 * @param collector
	 */
	@SuppressWarnings("rawtypes")
	public final void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
		if (this.fsUrl == null) {
			throw new IllegalStateException("File system URL must be specified.");
		}

		this.collector = collector;
		this.hdfsConfig = new Configuration();
		Map<String, Object> map = (Map<String, Object>) conf.get(this.configKey);
		if (map != null) {
			for (String key : map.keySet()) {
				this.hdfsConfig.set(key, String.valueOf(map.get(key)));
			}
		}

		try {
			HdfsSecurityUtil.login(conf, hdfsConfig);
			doPrepare(conf, topologyContext, collector);

		} catch (Exception e) {
			throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
	
	abstract void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException;
}
