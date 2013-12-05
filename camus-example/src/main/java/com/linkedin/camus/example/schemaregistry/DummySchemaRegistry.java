package com.linkedin.camus.example.schemaregistry;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import net.daum.shopping.gargoyle.entity.common.log.ClickLogAvro;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class DummySchemaRegistry extends MemorySchemaRegistry<Schema> {
	public DummySchemaRegistry(Configuration conf) {
		super();
        super.register("ggLogTest", ClickLogAvro.newBuilder().build().getSchema());
	}

    public DummySchemaRegistry() {
        super();
        super.register("ggLogTest", ClickLogAvro.newBuilder().build().getSchema());
    }
}
