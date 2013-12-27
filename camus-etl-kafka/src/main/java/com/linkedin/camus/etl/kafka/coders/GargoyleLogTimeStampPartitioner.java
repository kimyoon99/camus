package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created with IntelliJ IDEA.
 * User: unong
 * Date: 12/20/13
 * Time: 2:47 PM
 *
 * DefaultPartitioner 대신 사용
 * GargoyleLogTimeStampMessageDecoder 에서 CamusWrapper record 에 timestamp 를 데이터의 timestamp 를 찍어뒀기 때문에 해당 정보를 이용하여 파일을 분할할 수 있도록 수정
 *
 */
public class GargoyleLogTimeStampPartitioner  implements Partitioner {
    private static Logger log = Logger.getLogger(GargoyleLogTimeStampPartitioner.class);

    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/kk";
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        log.debug("UNONG encodePartition called");

        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
//        CamusWrapper value = (CamusWrapper) key.getPartitionMap().get("object");
//        log.debug("UNONG encodePartition object :: " + value.toString());

        return ""+DateUtils.getPartition(outfilePartitionMs, key.getTime());
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, int brokerId, int partitionId, String encodedPartition) {
        log.debug("UNONG generatePartitionedPath called");

        // We only need to initialize outputDateFormatter with the default timeZone once.
        if (outputDateFormatter == null) {
            outputDateFormatter = DateUtils.getDateTimeFormatter(
                    OUTPUT_DATE_FORMAT,
                    DateTimeZone.forID(EtlMultiOutputFormat.getDefaultTimeZone(context))
            );
        }

        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        sb.append(bucket.toString(OUTPUT_DATE_FORMAT));
        return sb.toString();
    }
}
