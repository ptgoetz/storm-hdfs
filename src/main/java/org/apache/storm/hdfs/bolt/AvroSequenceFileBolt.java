package org.apache.storm.hdfs.bolt;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.bolt.format.AvroSequenceFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroSequenceFileBolt extends AbstractHdfsBolt {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSequenceFileBolt.class);

  private AvroSequenceFormat format;
  private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.RECORD;
  private transient SequenceFile.Writer writer;

  private String compressionCodec = "default";
  private transient CompressionCodecFactory codecFactory;

  public AvroSequenceFileBolt() {
  }

  public AvroSequenceFileBolt withCompressionCodec(String codec){
    this.compressionCodec = codec;
    return this;
  }

  public AvroSequenceFileBolt withFsUrl(String fsUrl) {
    this.fsUrl = fsUrl;
    return this;
  }

  public AvroSequenceFileBolt withConfigKey(String configKey){
    this.configKey = configKey;
    return this;
  }

  public AvroSequenceFileBolt withFileNameFormat(FileNameFormat fileNameFormat) {
    this.fileNameFormat = fileNameFormat;
    return this;
  }

  public AvroSequenceFileBolt withSequenceFormat(AvroSequenceFormat format) {
    this.format = format;
    return this;
  }

  public AvroSequenceFileBolt withSyncPolicy(SyncPolicy syncPolicy) {
    this.syncPolicy = syncPolicy;
    return this;
  }

  public AvroSequenceFileBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
    this.rotationPolicy = rotationPolicy;
    return this;
  }

  public AvroSequenceFileBolt withCompressionType(SequenceFile.CompressionType compressionType){
    this.compressionType = compressionType;
    return this;
  }

  public AvroSequenceFileBolt addRotationAction(RotationAction action){
    this.rotationActions.add(action);
    return this;
  }

  @Override
  public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
    LOG.info("Preparing Sequence File Bolt...");
    if (this.format == null) throw new IllegalStateException("SequenceFormat must be specified.");

    this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
    this.codecFactory = new CompressionCodecFactory(hdfsConfig);
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      long offset;
      synchronized (this.writeLock) {
        this.writer.append(this.format.key(tuple), this.format.value(tuple));
        offset = this.writer.getLength();

        if (this.syncPolicy.mark(tuple, offset)) {
          this.writer.hsync();
          this.syncPolicy.reset();
        }
      }

      this.collector.ack(tuple);
      if (this.rotationPolicy.mark(tuple, offset)) {
        rotateOutputFile(); // synchronized
        this.rotationPolicy.reset();
      }
    } catch (IOException e) {
      LOG.error("write/sync failed.", e);
      this.collector.fail(tuple);
    }

  }

  Path createOutputFile() throws IOException {
    Path p = new Path(this.fsUrl + this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
    AvroSequenceFile.Writer.Options options = new AvroSequenceFile.Writer.Options()
        .withConfiguration(this.hdfsConfig)
        .withFileSystem(this.fs)
        .withOutputPath(p)
        .withCompressionType(this.compressionType)
        .withCompressionCodec(this.codecFactory.getCodecByName(this.compressionCodec));
    if (this.format.keySchema() != null) {
      options.withKeySchema(this.format.keySchema());
    } else {
      options.withKeyClass(this.format.keyClass());
    }
    if (this.format.valueSchema() != null) {
      options.withValueSchema(this.format.valueSchema());
    } else {
      options.withValueClass(this.format.valueClass());
    }
    this.writer = AvroSequenceFile.createWriter(options);
    return p;
  }

  void closeOutputFile() throws IOException {
    this.writer.close();
  }

}
