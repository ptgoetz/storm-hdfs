package org.apache.storm.hdfs.bolt.rotation;


import org.apache.storm.hdfs.common.filemanager.FileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.tuple.Tuple;

import java.util.concurrent.TimeUnit;

/**
 * File rotation policy that will rotate files after a certain
 * amount of time has pass
 *
 * For example:
 * <pre>
 *     // rotate files every 30 minutes
 *     TimeRotationPolicy policy =
 *          new TimeRotationPolicy(30.0, Units.MINUTES, FileManager);
 * </pre>
 *
 */
public class TimeBasedRotationPolicy implements FileRotationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(TimeBasedRotationPolicy.class);

    private final int value;
    private final TimeUnit unit;

    public TimeBasedRotationPolicy(int value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public void prepare (final FileManager fileManager) {
        new Thread("TimeBasedRotation-timer-thread"){
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(unit.toMillis(value));
                        fileManager.rotate();
                    } catch (Exception e) {
                        LOG.warn("Failed to rotate the file.", e);
                    }
                }
            }
        }.start();
    }

    /**
     * Always returns false as the rotation happens asynchronously in timer thread.
     * @param tuple The tuple executed.
     * @param offset current offset of file being written
     * @return always false
     */
    @Override
    public boolean mark(Tuple tuple, long offset) {
        return false;
    }

    /**
     *
     */
    @Override
    public void reset() {
        throw new UnsupportedOperationException("Should never be called");
    }
}