package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapreduce.Mapper;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.log4j.Logger;

/**
 * KafkaETL mapper
 * 
 * input -- EtlKey, AvroWrapper
 * 
 * output -- EtlKey, AvroWrapper
 * 
 */
public class EtlMapper extends Mapper<EtlKey, AvroWrapper<Object>, EtlKey, AvroWrapper<Object>> {
    private static org.apache.log4j.Logger log = Logger
            .getLogger(EtlMapper.class);

	@Override
	public void map(EtlKey key, AvroWrapper<Object> val, Context context) throws IOException, InterruptedException {
        System.out.println("c8 call map !!!");
        log.error("UNONG etlKey : " + key.toString());

		long startTime = System.currentTimeMillis();

		context.write(key, val);

		long endTime = System.currentTimeMillis();
		long mapTime = ((endTime - startTime));
		context.getCounter("total", "mapper-time(ms)").increment(mapTime);
	}
}
