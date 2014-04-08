package com.linkedin.camus.schemaregistry;

import org.apache.avro.Schema;

/**
 * Created by unong on 4/8/14.
 */
public interface AvroSchemaIdRegistry {
    public String getSchemaId(Schema schema);
}
