package com.linkedin.camus.example.schemaregistry;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import net.daum.shopping.gargoyle.entity.common.log.ClickLogAvro;
import net.daum.shopping.gargoyle.entity.common.log.ImpressionLogAvro;
import net.daum.shopping.gargoyle.entity.common.log.OldClickLogAvro;
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
        super.register("gClickLogResult", ClickLogAvro.newBuilder().build().getSchema());
        super.register("gImpressionLogResult", ImpressionLogAvro.newBuilder().build().getSchema());
    }

    public GargoyleSchemaRegistry()  {
        this(null);
    }


    @Override
    public Schema getSchemaByID(String topicName, String idStr) {
        /**
         * 이걸 부르는 케이스는 예전 스키마에 대한 요청임.
         */
        if("gClickLogResult".equals(topicName)) {
            return OldClickLogAvro.newBuilder().build().getSchema();
        } else if("gImpressionLogResult".equals(topicName)) {
            return ImpressionLogAvro.newBuilder().build().getSchema();
        }
        return null;
    }
}
