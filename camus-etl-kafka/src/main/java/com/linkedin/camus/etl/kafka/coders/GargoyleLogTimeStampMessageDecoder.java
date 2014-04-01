package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: unong
 * Date: 12/20/13
 * Time: 1:57 PM
 *
 * LatestSchemaKafkaAvroMessageDecoder 과 비슷한걸 만듬
 * 하지만 payload 를 파싱해서 만든 데이터에서 logdttm 값을 뽑아서 데이터의 시간에 따라 파티셔닝 할 수 있도록 decoding
 *
 */
public class GargoyleLogTimeStampMessageDecoder extends MessageDecoder<byte[], SpecificRecordBase> {
    private static Logger log = Logger.getLogger(GargoyleLogTimeStampMessageDecoder.class);

    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;
    private Schema latestSchema;

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);
        try {
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class
                    .forName(
                            props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();

            registry.init(props);

            this.registry = new CachedSchemaRegistry<Schema>(registry);
            this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();
        } catch (Exception e) {
            log.error(null, e);
            throw new MessageDecoderException(e);
        }
        decoderFactory = DecoderFactory.get();
    }

    @Override
    public CamusWrapper<SpecificRecordBase> decode(byte[] payload)
    {
        try
        {
            SpecificDatumReader<SpecificRecordBase> reader = new SpecificDatumReader<SpecificRecordBase>();

            Schema schema = registry.getLatestSchemaByTopic(super.topicName).getSchema();
            reader.setSchema(schema);

            log.debug("UNONG payloadString :: " + 0 + "(" + (payload.length) + ")");
            SpecificRecordBase record = reader.read(
                    null,
                    decoderFactory.binaryDecoder(
                            payload,
                            null
                    )
            );

//            Utf8 logdttm = (Utf8)record.get(schema.getField("log_dttm").pos());
//            log.info("UNONG logdttm :: " + logdttm.toString());
//            long timestamp = DateUtils.getDateTimeFormatter("yyyyMMddkkmmss").parseMillis(logdttm.toString());
//            log.info("UNONG payload logdttm=" + logdttm + ", timestamp=" + timestamp);

            Long log_timestamp = (Long) record.get(schema.getField("log_timestamp").pos());
            log.info("UNONG payload logdttm=" + log_timestamp);

            return new CamusWrapper<SpecificRecordBase>(record, log_timestamp);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
