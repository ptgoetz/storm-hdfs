package org.apache.storm.hdfs.trident.format;

import org.apache.hadoop.io.Writable;
import org.apache.storm.hdfs.common.format.CommonSequenceFormat;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * Interface for converting <code>TridentTuple</code> objects to HDFS sequence file key-value pairs.
 *
 */
public interface SequenceFormat extends CommonSequenceFormat {

    /**
     * Given a tuple, return the key that should be written to the sequence file.
     *
     * @param tuple
     * @return
     */
    Writable key(TridentTuple tuple);

    /**
     * Given a tuple, return the value that should be written to the sequence file.
     * @param tuple
     * @return
     */
    Writable value(TridentTuple tuple);
}
