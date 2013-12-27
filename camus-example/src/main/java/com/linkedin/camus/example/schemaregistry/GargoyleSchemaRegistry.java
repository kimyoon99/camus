package com.linkedin.camus.example.schemaregistry;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import net.daum.shopping.gargoyle.entity.common.log.ClickLogAvro;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

/**
 * Created with IntelliJ IDEA.
 * User: unong
 * Date: 12/5/13
 * Time: 12:47 PM
 */
public class GargoyleSchemaRegistry extends MemorySchemaRegistry<Schema> {
    public GargoyleSchemaRegistry(Configuration conf) {
        super();
        super.register("ggLogTest", ClickLogAvro.newBuilder().build().getSchema());
        super.register("gClickLogResult", ClickLogAvro.newBuilder().build().getSchema());
    }

    public GargoyleSchemaRegistry()  {
        this(null);
    }
}
