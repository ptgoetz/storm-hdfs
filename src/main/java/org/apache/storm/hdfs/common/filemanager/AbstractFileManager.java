package org.apache.storm.hdfs.common.filemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.common.format.CommonFileNameFormat;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractFileManager implements FileManager {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileManager.class);

    protected CommonFileNameFormat fileNameFormat;
    protected String fsUrl;
    protected List<RotationAction> rotationActions = new ArrayList<RotationAction>(1);
    protected Configuration hdfsConfig;
    protected long rotation;
    protected Path currentFile;
    protected FileSystem fs;
    protected long offset;

    @Override
    public synchronized void rotate() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        closeOutputFile();
        this.rotation++;

        Path newFile = createOutputFile();
        LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
        for(RotationAction action : this.rotationActions){
            action.execute(this.fs, this.currentFile);
        }

        this.currentFile = newFile;
        this.offset = 0;
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }
}
