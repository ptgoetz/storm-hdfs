package org.apache.storm.hdfs.common.format;


import java.io.Serializable;

public interface CommonSequenceFormat extends Serializable {
    /**
     * Key class used by implementation (e.g. IntWritable.class, etc.)
     *
     * @return
     */
    Class keyClass();

    /**
     * Value class used by implementation (e.g. Text.class, etc.)
     * @return
     */
    Class valueClass();
}
