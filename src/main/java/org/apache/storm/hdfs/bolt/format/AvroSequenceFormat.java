package org.apache.storm.hdfs.bolt.format;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.storm.tuple.Tuple;


public interface AvroSequenceFormat extends Serializable {
  /**
   * Key class used by implementation (e.g. IntWritable.class, etc.)
   *
   * @return
   */
  Schema keySchema();

  /**
   * Value class used by implementation (e.g. Text.class, etc.)
   * @return
   */
  Schema valueSchema();

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

  /**
   * Given a tuple, return the key that should be written to the sequence file.
   *
   * @param tuple
   * @return
   */
  Object key(Tuple tuple);

  /**
   * Given a tuple, return the value that should be written to the sequence file.
   * @param tuple
   * @return
   */
  Object value(Tuple tuple);
}
