package org.apache.storm.hdfs.common.filemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;
import org.apache.storm.hdfs.common.format.CommonFileNameFormat;
import org.apache.storm.hdfs.common.format.CommonSequenceFormat;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SequenceFileManager extends AbstractFileManager {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileManager.class);

    private CommonSequenceFormat format;
    private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.RECORD;
    private SequenceFile.Writer writer;
    private String compressionCodec = "default";
    private transient CompressionCodecFactory codecFactory;

    @Override
    public synchronized Path createOutputFile() throws IOException {
        this.currentFile = new Path(this.fsUrl + this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.writer = SequenceFile.createWriter(
                this.hdfsConfig,
                SequenceFile.Writer.file(currentFile),
                SequenceFile.Writer.keyClass(this.format.keyClass()),
                SequenceFile.Writer.valueClass(this.format.valueClass()),
                SequenceFile.Writer.compression(this.compressionType, this.codecFactory.getCodecByName(this.compressionCodec))
        );
        return this.currentFile;
    }

    @Override
    public synchronized void closeOutputFile() throws IOException {
        sync();
        this.writer.close();
    }

    @Override
    public synchronized long append(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("SequenceWriter only supports writing key, val pairs");
    }

    @Override
    public synchronized long append(Writable key, Writable val) throws IOException {
        this.writer.append(key, val);
        return writer.getLength();
    }

    @Override
    public synchronized void sync() throws IOException {
        this.writer.hsync();
    }

    public SequenceFileManager withFileNameFormat(final CommonFileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public SequenceFileManager withFsUrl(final String fsUrl) {
        this.fsUrl = fsUrl;
        return this;
    }

    public SequenceFileManager addRotationAction(final RotationAction rotationAction) {
        this.rotationActions.add(rotationAction);
        return this;
    }

    public SequenceFileManager withHdfsConfig(final Configuration hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
        return this;
    }

    public SequenceFileManager withFs(final FileSystem fs) {
        this.fs = fs;
        return this;
    }

    public SequenceFileManager withSequenceFormat(CommonSequenceFormat format) {
        this.format = format;
        return this;
    }

    public SequenceFileManager withCodecFactory(CompressionCodecFactory codecFactory) {
        this.codecFactory = codecFactory;
        return this;
    }

    public SequenceFileManager withCompressionType(SequenceFile.CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public SequenceFileManager withCompressionCodec(String codec) {
        this.compressionCodec = codec;
        return this;
    }
}
