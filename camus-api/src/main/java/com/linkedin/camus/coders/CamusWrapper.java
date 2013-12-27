package com.linkedin.camus.coders;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 *
 * @author kgoodhop
 *
 * @param <R> The type of decoded payload
 */
public class CamusWrapper<R> {
    private static Logger log = Logger.getLogger(CamusWrapper.class);

    private R record;
    private long timestamp;
    private MapWritable partitionMap;

    public CamusWrapper(R record) {
        this(record, System.currentTimeMillis());
        log.info("UNONG record :: " + record.toString() + " :: " + record.getClass().getName());
    }

    public CamusWrapper(R record, long timestamp) {
        this(record, timestamp, "unknown_server", "unknown_service");
    }

    public CamusWrapper(R record, long timestamp, String server, String service) {
        this.record = record;
        this.timestamp = timestamp;
        this.partitionMap = new MapWritable();
        partitionMap.put(new Text("server"), new Text(server));
        partitionMap.put(new Text("service"), new Text(service));
    }

    /**
     * Returns the payload record for a single message
     * @return
     */
    public R getRecord() {
        return record;
    }

    /**
     * Returns current if not set by the decoder
     * @return
     */
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Add a value for partitions
     */
    public void put(Writable key, Writable value) {
        partitionMap.put(key, value);
    }

    /**
     * Get a value for partitions
     * @return the value for the given key
     */
    public Writable get(Writable key) {
        return partitionMap.get(key);
    }

    /**
     * Get all the partition key/partitionMap
     */
    public MapWritable getPartitionMap() {
        return partitionMap;
    }

    @Override
    public String toString() {
        return "CamusWrapper{" +
                "partitionMap=" + partitionMap +
                ", record=" + record +
                ", timestamp=" + timestamp +
                '}';
    }
}
