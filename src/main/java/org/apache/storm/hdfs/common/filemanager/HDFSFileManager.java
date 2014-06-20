package org.apache.storm.hdfs.common.filemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.common.format.CommonFileNameFormat;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;


public class HDFSFileManager extends AbstractFileManager {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileManager.class);

    private FSDataOutputStream out;

    @Override
    public synchronized void closeOutputFile() throws IOException {
        sync();
        this.out.close();
    }

    @Override
    public synchronized Path createOutputFile() throws IOException {
        this.currentFile = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(currentFile);
        return currentFile;
    }

    @Override
    public synchronized long append(byte[] bytes) throws IOException {
        out.write(bytes);
        this.offset += bytes.length;
        return this.offset;
    }

    @Override
    public synchronized long append(Writable key, Writable val) throws IOException {
        throw new UnsupportedOperationException("Use SequenceFileManager for writing key-> val.");
    }

    @Override
    public synchronized void sync() throws IOException {
        if(this.out instanceof HdfsDataOutputStream){
            ((HdfsDataOutputStream)this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
    }

    public HDFSFileManager withFileNameFormat(final CommonFileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HDFSFileManager withFsUrl(final String fsUrl) {
        this.fsUrl = fsUrl;
        return this;
    }

    public HDFSFileManager addRotationAction(final RotationAction rotationAction) {
        this.rotationActions.add(rotationAction);
        return this;
    }

    public HDFSFileManager withHdfsConfig(final Configuration hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
        return this;
    }

    public HDFSFileManager withFs(final FileSystem fs) {
        this.fs = fs;
        return this;
    }
}
