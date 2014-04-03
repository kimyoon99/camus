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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

public class KafkaAvroMessageDecoder extends MessageDecoder<byte[], SpecificRecordBase> {
    private static final Logger log = Logger.getLogger(KafkaAvroMessageDecoder.class);

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

	private class MessageDecoderHelper {
		//private Message message;
		private ByteBuffer buffer;
		private Schema schema;
		private int start;
		private int length;
		private Schema targetSchema;
		private static final byte MAGIC_BYTE = 0x0;
		private final SchemaRegistry<Schema> registry;
		private final String topicName;
		private byte[] payload;

		public MessageDecoderHelper(SchemaRegistry<Schema> registry,
				String topicName, byte[] payload) {
			this.registry = registry;
			this.topicName = topicName;
			//this.message = message;
			this.payload = payload;
		}

		public ByteBuffer getBuffer() {
			return buffer;
		}

		public Schema getSchema() {
			return schema;
		}

		public int getStart() {
			return start;
		}

		public int getLength() {
			return length;
		}

		public Schema getTargetSchema() {
			return targetSchema;
		}

		private ByteBuffer getByteBuffer(byte[] payload) {
			ByteBuffer buffer = ByteBuffer.wrap(payload);
			if (buffer.get() != MAGIC_BYTE)
				throw new IllegalArgumentException("Unknown magic byte!");
			return buffer;
		}

		public MessageDecoderHelper invoke() {
//			buffer = getByteBuffer(payload);
//			String id = Integer.toString(buffer.getInt());
//            log.info("UNONG schema id " + id);
			schema = registry.getSchemaByID(topicName, null);
			if (schema == null)
				throw new IllegalStateException("Unknown schema id: null");

//			start = buffer.position() + buffer.arrayOffset();
//			length = buffer.limit() - 5;

			// try to get a target schema, if any
			targetSchema = latestSchema;
			return this;
		}
	}

    @Override
    public CamusWrapper<SpecificRecordBase> decode(byte[] payload) {
        log.info("UNONG decoding ");
		try {
			MessageDecoderHelper helper = new MessageDecoderHelper(registry,
					topicName, payload).invoke();

            SpecificDatumReader<SpecificRecordBase> reader = (helper.getTargetSchema() == null) ? new SpecificDatumReader<SpecificRecordBase>(
					helper.getSchema()) : new SpecificDatumReader<SpecificRecordBase>(
					helper.getSchema(), helper.getTargetSchema());

            log.info("UNONG decoding " + reader.getSchema().getName() + " :: " + reader.getExpected().getName());
			return new CamusAvroWrapper(reader.read(null, decoderFactory
                    .binaryDecoder(payload, null)), reader.getSchema());
//                    .binaryDecoder(helper.getBuffer().array(),
//                            helper.getStart(), helper.getLength(), null)));
	
		} catch (IOException e) {
			throw new MessageDecoderException(e);
		}
	}

	public static class CamusAvroWrapper extends CamusWrapper<SpecificRecordBase> {
        private Schema schema;
	    public CamusAvroWrapper(SpecificRecordBase record, Schema schema) {
            super(record);
            this.schema = schema;
//            SpecificRecordBase header = (SpecificRecordBase) super.getRecord().get("header");
//   	        if (header != null) {
//               if (header.get("server") != null) {
//                   put(new Text("server"), new Text(header.get("server").toString()));
//               }
//               if (header.get("service") != null) {
//                   put(new Text("service"), new Text(header.get("service").toString()));
//               }
//            }
        }
	    
	    @Override
	    public long getTimestamp() {
	        if (super.getRecord().get(schema.getField("log_timestamp").pos()) != null) {
                log.info("UNONG log_timestamp :: " + (Long) super.getRecord().get(schema.getField("log_timestamp").pos()) );
	            return (Long) super.getRecord().get(schema.getField("log_timestamp").pos());
	        } else {
	            return System.currentTimeMillis();
	        }
	    }
	}
}
