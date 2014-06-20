package org.apache.storm.hdfs.bolt.format;

import backtype.storm.tuple.Tuple;
import org.apache.hadoop.io.Writable;
import org.apache.storm.hdfs.common.format.CommonSequenceFormat;

import java.io.Serializable;

/**
 * Interface for converting <code>Tuple</code> objects to HDFS sequence file key-value pairs.
 *
 */
public interface SequenceFormat extends CommonSequenceFormat {
    /**
     * Given a tuple, return the key that should be written to the sequence file.
     *
     * @param tuple
     * @return
     */
    Writable key(Tuple tuple);

    /**
     * Given a tuple, return the value that should be written to the sequence file.
     * @param tuple
     * @return
     */
    Writable value(Tuple tuple);
}
