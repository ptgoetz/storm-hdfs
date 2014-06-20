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
package org.apache.storm.hdfs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimeBasedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.filemanager.FileManager;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public abstract class AbstractHdfsBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsBolt.class);

    protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
    protected OutputCollector collector;
    protected FileSystem fs;
    protected SyncPolicy syncPolicy;
    protected FileRotationPolicy rotationPolicy;
    protected FileNameFormat fileNameFormat;
    protected String fsUrl;
    protected String configKey;
    protected FileManager fileManager;
    protected Configuration hdfsConfig;

    /**
     * Marked as final to prevent override. Subclasses should implement the doPrepare() method.
     * @param conf
     * @param topologyContext
     * @param collector
     */
    public final void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector){
        if (this.syncPolicy == null) throw new IllegalStateException("SyncPolicy must be specified.");
        if (this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");
        if (this.fsUrl == null) {
            throw new IllegalStateException("File system URL must be specified.");
        }

        this.collector = collector;
        this.fileNameFormat.prepare(conf, topologyContext);
        this.hdfsConfig = new Configuration();
        Map<String, Object> map = (Map<String, Object>)conf.get(this.configKey);
        if(map != null){
            for(String key : map.keySet()){
                this.hdfsConfig.set(key, String.valueOf(map.get(key)));
            }
        }

        try{
            HdfsSecurityUtil.login(conf, hdfsConfig);
            doPrepare(conf, topologyContext, collector);

            if(this.rotationPolicy instanceof TimeBasedRotationPolicy) {
                ((TimeBasedRotationPolicy)this.rotationPolicy).prepare(fileManager);
            }

            fileManager.createOutputFile();
        } catch (Exception e){
            throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    abstract void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException;

}
