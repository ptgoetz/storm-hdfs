package org.apache.storm.hdfs.common.filemanager;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public interface FileManager {

    void closeOutputFile() throws IOException;

    Path createOutputFile() throws IOException;

    long append(byte[] bytes) throws IOException;

    long append(Writable key, Writable val) throws IOException;

    void rotate() throws IOException;

    void sync() throws IOException;
}
