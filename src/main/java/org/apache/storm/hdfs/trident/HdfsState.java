package org.apache.storm.hdfs.trident;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.trident.rotation.TimeBasedRotationPolicy;
import org.apache.storm.hdfs.common.filemanager.FileManager;
import org.apache.storm.hdfs.common.filemanager.HDFSFileManager;
import org.apache.storm.hdfs.common.filemanager.SequenceFileManager;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.format.SequenceFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsState implements State {

    public static abstract class Options implements Serializable {

        protected String fsUrl;
        protected String configKey;
        protected FileSystem fs;
        protected FileRotationPolicy rotationPolicy;
        protected FileNameFormat fileNameFormat;
        protected Configuration hdfsConfig;
        protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
        protected FileManager fileManager;

        abstract void execute(List<TridentTuple> tuples) throws IOException;

        abstract void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException;

        void prepare(Map conf, int partitionIndex, int numPartitions){
            if (this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");
            if (this.fsUrl == null) {
                throw new IllegalStateException("File system URL must be specified.");
            }
            this.fileNameFormat.prepare(conf, partitionIndex, numPartitions);
            this.hdfsConfig = new Configuration();
            Map<String, Object> map = (Map<String, Object>)conf.get(this.configKey);
            if(map != null){
                for(String key : map.keySet()){
                    this.hdfsConfig.set(key, String.valueOf(map.get(key)));
                }
            }

            try {
                HdfsSecurityUtil.login(conf, hdfsConfig);
                doPrepare(conf, partitionIndex, numPartitions);

                if(this.rotationPolicy instanceof TimeBasedRotationPolicy) {
                    ((TimeBasedRotationPolicy)this.rotationPolicy).prepare(fileManager);
                }

                fileManager.createOutputFile();
            } catch (Exception e){
                throw new RuntimeException("Error preparing HdfsState: " + e.getMessage(), e);
            }
        }

    }

    public static class HdfsFileOptions extends Options {

        protected RecordFormat format;

        public HdfsFileOptions withFsUrl(String fsUrl){
            this.fsUrl = fsUrl;
            return this;
        }

        public HdfsFileOptions withConfigKey(String configKey){
            this.configKey = configKey;
            return this;
        }

        public HdfsFileOptions withFileNameFormat(FileNameFormat fileNameFormat){
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public HdfsFileOptions withRecordFormat(RecordFormat format){
            this.format = format;
            return this;
        }

        public HdfsFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy){
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        public HdfsFileOptions addRotationAction(RotationAction action){
            this.rotationActions.add(action);
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing HDFS Bolt...");
            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
            HDFSFileManager hdfsFileManager = new HDFSFileManager()
                    .withFileNameFormat(this.fileNameFormat)
                    .withFsUrl(this.fsUrl)
                    .withHdfsConfig(this.hdfsConfig)
                    .withFs(this.fs);
            for(RotationAction rotationAction : rotationActions) {
                hdfsFileManager.addRotationAction(rotationAction);
            }

            this.fileManager = hdfsFileManager;
        }

        @Override
        public void execute(List<TridentTuple> tuples) throws IOException {
            for(TridentTuple tuple : tuples){
                byte[] bytes = this.format.format(tuple);
                long offset = fileManager.append(bytes);
                if(this.rotationPolicy.mark(tuple, offset)){
                    fileManager.rotate();
                    this.rotationPolicy.reset();
                }
            }
        }
    }

    public static class SequenceFileOptions extends Options {
        private SequenceFormat format;
        private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.RECORD;
        private String compressionCodec = "default";
        private transient CompressionCodecFactory codecFactory;

        public SequenceFileOptions withCompressionCodec(String codec){
            this.compressionCodec = codec;
            return this;
        }

        public SequenceFileOptions withFsUrl(String fsUrl) {
            this.fsUrl = fsUrl;
            return this;
        }

        public SequenceFileOptions withConfigKey(String configKey){
            this.configKey = configKey;
            return this;
        }

        public SequenceFileOptions withFileNameFormat(FileNameFormat fileNameFormat) {
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public SequenceFileOptions withSequenceFormat(SequenceFormat format) {
            this.format = format;
            return this;
        }

        public SequenceFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy) {
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        public SequenceFileOptions withCompressionType(SequenceFile.CompressionType compressionType){
            this.compressionType = compressionType;
            return this;
        }

        public SequenceFileOptions addRotationAction(RotationAction action){
            this.rotationActions.add(action);
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing Sequence File State...");
            if (this.format == null) throw new IllegalStateException("SequenceFormat must be specified.");

            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
            this.codecFactory = new CompressionCodecFactory(hdfsConfig);

            SequenceFileManager sequenceFileManager = new SequenceFileManager()
                    .withFileNameFormat(this.fileNameFormat)
                    .withFsUrl(this.fsUrl)
                    .withHdfsConfig(this.hdfsConfig)
                    .withFs(this.fs)
                    .withSequenceFormat(this.format)
                    .withCompressionCodec(this.compressionCodec)
                    .withCompressionType(this.compressionType)
                    .withCodecFactory(codecFactory);
            for(RotationAction rotationAction : rotationActions) {
                sequenceFileManager.addRotationAction(rotationAction);
            }

            this.fileManager = sequenceFileManager;
        }

        @Override
        public void execute(List<TridentTuple> tuples) throws IOException {
            for(TridentTuple tuple : tuples) {
                long offset = fileManager.append(this.format.key(tuple), this.format.value(tuple));

                if (this.rotationPolicy.mark(tuple, offset)) {
                    fileManager.rotate();
                    this.rotationPolicy.reset();
                }
            }
        }

    }

    public static final Logger LOG = LoggerFactory.getLogger(HdfsState.class);
    private Options options;

    HdfsState(Options options){
        this.options = options;
    }

    void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions){
        this.options.prepare(conf, partitionIndex, numPartitions);
    }

    @Override
    public void beginCommit(Long txId) {
    }

    @Override
    public void commit(Long txId) {
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector){
        try{
            this.options.execute(tuples);
        } catch (IOException e){
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }
}
