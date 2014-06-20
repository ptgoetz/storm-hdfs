package org.apache.storm.hdfs.common.format;

import java.io.Serializable;

public interface CommonFileNameFormat extends Serializable {

    /**
     * Returns the filename the HdfsBolt will create.
     * @param rotation the current file rotation number (incremented on every rotation)
     * @param timeStamp current time in milliseconds when the rotation occurs
     * @return
     */
    String getName(long rotation, long timeStamp);

    String getPath();
}
