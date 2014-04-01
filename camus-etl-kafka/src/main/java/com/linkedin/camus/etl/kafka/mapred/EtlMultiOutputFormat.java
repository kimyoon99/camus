package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MultipleAvroOutputFormat.
 *
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {
    public static final String ETL_DESTINATION_PATH = "etl.destination.path";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
    public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
    public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";

    public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
    public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";
    public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
    public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";

    public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";
    public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";
    public static final String ETL_OUTPUT_CODEC = "etl.output.codec";
    public static final String ETL_DEFAULT_OUTPUT_CODEC = "deflate";
    public static final String ETL_RECORD_WRITER_PROVIDER_CLASS = "etl.record.writer.provider.class";

    public static final DateTimeFormatter FILE_DATE_FORMATTER = DateUtils
            .getDateTimeFormatter("YYYYMMddkk");
    public static final String OFFSET_PREFIX = "offsets";
    public static final String ERRORS_PREFIX = "errors";
    public static final String COUNTS_PREFIX = "counts";

    public static final String REQUESTS_FILE = "requests.previous";
    private static EtlMultiOutputCommitter committer = null;
    private static Map<String, Partitioner> partitionersByTopic = new HashMap<String, Partitioner>();

    private long granularityMs;
    private static Logger log = Logger.getLogger(EtlMultiOutputFormat.class);

    @Override
    public RecordWriter<EtlKey, Object> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        log.debug("UNONG getRecordWriter called");
        if (committer == null)
            committer = new EtlMultiOutputCommitter(getOutputPath(context), context);
        granularityMs = getMonitorTimeGranularityMins(context) * 60000L;
        return new MultiEtlRecordWriter(context);
    }

    private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapper value) throws IOException,
            InterruptedException {
        log.debug("UNONG getDataRecordWriter called");
        RecordWriterProvider recordWriterProvider = null;
        try {
            recordWriterProvider = getRecordWriterProviderClass(context).newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return recordWriterProvider.getDataRecordWriter(context, fileName, value, committer);
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException {
        log.debug("UNONG getOutputCommitter called");
        if (committer == null)
            committer = new EtlMultiOutputCommitter(getOutputPath(context), context);
        granularityMs = getMonitorTimeGranularityMins(context) * 60 * 1000L;
        return committer;
    }

    public static void setRecordWriterProviderClass(JobContext job, Class<RecordWriterProvider> recordWriterProviderClass) {
        log.debug("UNONG setRecordWriterProviderClass called");
        job.getConfiguration().setClass(ETL_RECORD_WRITER_PROVIDER_CLASS, recordWriterProviderClass, RecordWriterProvider.class);
    }

    public static Class<RecordWriterProvider> getRecordWriterProviderClass(
            JobContext job) {
        log.debug("UNONG getRecordWriterProviderClass called");
        return (Class<RecordWriterProvider>) job.getConfiguration()
                .getClass(ETL_RECORD_WRITER_PROVIDER_CLASS,
                        AvroRecordWriterProvider.class);
    }

    public static void setDefaultTimeZone(JobContext job, String tz) {
        log.debug("UNONG setDefaultTimeZone called");
        job.getConfiguration().set(ETL_DEFAULT_TIMEZONE, tz);
    }

    public static String getDefaultTimeZone(JobContext job) {
        log.debug("UNONG getDefaultTimeZone called");
        return job.getConfiguration().get(ETL_DEFAULT_TIMEZONE, "America/Los_Angeles");
    }

    public static void setDestinationPath(JobContext job, Path dest) {
        log.debug("UNONG setDestinationPath called");
        job.getConfiguration().set(ETL_DESTINATION_PATH, dest.toString());
    }

    public static Path getDestinationPath(JobContext job) {
        log.debug("UNONG getDestinationPath called");
        return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH));
    }

    public static void setDestPathTopicSubDir(JobContext job, String subPath) {
        log.debug("UNONG setDestPathTopicSubDir called");
        job.getConfiguration().set(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, subPath);
    }

    public static Path getDestPathTopicSubDir(JobContext job) {
        log.debug("UNONG getDestPathTopicSubDir called");
        return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, "hourly"));
    }

    public static void setMonitorTimeGranularityMins(JobContext job, int mins) {
        log.debug("UNONG setMonitorTimeGranularityMins called");
        job.getConfiguration().setInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, mins);
    }

    public static int getMonitorTimeGranularityMins(JobContext job) {
        log.debug("UNONG getMonitorTimeGranularityMins called");
        return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, 10);
    }

    public static void setEtlAvroWriterSyncInterval(JobContext job, int val) {
        log.debug("UNONG setEtlAvroWriterSyncInterval called");
        job.getConfiguration().setInt(ETL_AVRO_WRITER_SYNC_INTERVAL, val);
    }

    public static int getEtlAvroWriterSyncInterval(JobContext job) {
        log.debug("UNONG getEtlAvroWriterSyncInterval called");
        return job.getConfiguration().getInt(ETL_AVRO_WRITER_SYNC_INTERVAL, 16000);
    }

    public static void setEtlDeflateLevel(JobContext job, int val) {
        log.debug("UNONG setEtlDeflateLevel called");
        job.getConfiguration().setInt(ETL_DEFLATE_LEVEL, val);
    }

    public static void setEtlOutputCodec(JobContext job, String codec) {
        log.debug("UNONG setEtlOutputCodec called");
        job.getConfiguration().set(ETL_OUTPUT_CODEC, codec);
    }

    public static String getEtlOutputCodec(JobContext job) {
        log.debug("UNONG getEtlOutputCodec called");
        return job.getConfiguration().get(ETL_OUTPUT_CODEC, ETL_DEFAULT_OUTPUT_CODEC);

    }
    public static int getEtlDeflateLevel(JobContext job) {
        log.debug("UNONG getEtlDeflateLevel called");
        return job.getConfiguration().getInt(ETL_DEFLATE_LEVEL, 6);
    }

    public static int getEtlOutputFileTimePartitionMins(JobContext job) {
        log.debug("UNONG getEtlOutputFileTimePartitionMins called");
        return job.getConfiguration().getInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60);
    }

    public static void setEtlOutputFileTimePartitionMins(JobContext job, int val) {
        log.debug("UNONG setEtlOutputFileTimePartitionMins called");
        job.getConfiguration().setInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, val);
    }

    public static boolean isRunMoveData(JobContext job) {
        log.debug("UNONG isRunMoveData called");
        return job.getConfiguration().getBoolean(ETL_RUN_MOVE_DATA, true);
    }

    public static void setRunMoveData(JobContext job, boolean value) {
        log.debug("UNONG setRunMoveData called");
        job.getConfiguration().setBoolean(ETL_RUN_MOVE_DATA, value);
    }

    public static boolean isRunTrackingPost(JobContext job) {
        log.debug("UNONG isRunTrackingPost called");
        return job.getConfiguration().getBoolean(ETL_RUN_TRACKING_POST, false);
    }

    public static void setRunTrackingPost(JobContext job, boolean value) {
        log.debug("UNONG setRunTrackingPost called");
        job.getConfiguration().setBoolean(ETL_RUN_TRACKING_POST, value);
    }

    public String getWorkingFileName(JobContext context, EtlKey key) throws IOException {
        log.debug("UNONG getWorkingFileName called");
        Partitioner partitioner = getPartitioner(context, key.getTopic());

        // TODO unong : 여기 데이터의 시간 정보도 나오게..
        return "data." + key.getTopic().replaceAll("\\.", "_") + "." + key.getLeaderId() + "." + key.getPartition() + "." + partitioner.encodePartition(context, key);
    }

/*    public static Partitioner getDefaultPartitioner(JobContext job) {
        if(partitionersByTopic.get(ETL_DEFAULT_PARTITIONER_CLASS) == null) {
            //List<Partitioner> partitioners = job.getConfiguration().getInstances(ETL_DEFAULT_PARTITIONER_CLASS, com.linkedin.camus.coders.Partitioner.class);
            List<Partitioner> partitioners = new ArrayList<Partitioner>();
            partitioners.add(new DefaultPartitioner());
            partitionersByTopic.put(ETL_DEFAULT_PARTITIONER_CLASS, partitioners.get(0));
        }
        return partitionersByTopic.get(ETL_DEFAULT_PARTITIONER_CLASS);
    }*/
    
    public static Partitioner getDefaultPartitioner(JobContext job) {
        log.debug("UNONG getDefaultPartitioner called");
        if(partitionersByTopic.get(ETL_DEFAULT_PARTITIONER_CLASS) == null) {
            List<Partitioner> partitioners = job.getConfiguration().getInstances(ETL_DEFAULT_PARTITIONER_CLASS, com.linkedin.camus.coders.Partitioner.class);
            partitionersByTopic.put(ETL_DEFAULT_PARTITIONER_CLASS, partitioners.get(0));
        }
        return partitionersByTopic.get(ETL_DEFAULT_PARTITIONER_CLASS);
    }    

    public static Partitioner getPartitioner(JobContext job, String topicName) throws IOException {
        log.debug("UNONG getPartitioner called");
        String customPartitionerProperty = ETL_DEFAULT_PARTITIONER_CLASS + "." + topicName;
        if(partitionersByTopic.get(customPartitionerProperty) == null) {
            List<Partitioner> partitioners = new ArrayList<Partitioner>();//job.getConfiguration().getInstances(customPartitionerProperty, com.linkedin.camus.coders.Partitioner.class);
            if(partitioners.isEmpty()) {
                return getDefaultPartitioner(job);
            } else {
                partitionersByTopic.put(customPartitionerProperty, partitioners.get(0));
            }
        }
        return partitionersByTopic.get(customPartitionerProperty);
    }

    public static void resetPartitioners() {
        log.debug("UNONG resetPartitioners called");
        partitionersByTopic = new HashMap<String, Partitioner>();
    }

    class MultiEtlRecordWriter extends RecordWriter<EtlKey, Object> {
        private TaskAttemptContext context;
        private Writer errorWriter = null;
        private String currentTopic = "";
        private long beginTimeStamp = 0;

        private HashMap<String, RecordWriter<IEtlKey, CamusWrapper>> dataWriters = new HashMap<String, RecordWriter<IEtlKey, CamusWrapper>>();

        public MultiEtlRecordWriter(TaskAttemptContext context) throws IOException,
                InterruptedException {
            log.debug("UNONG MultiEtlRecordWriter called");
            this.context = context;
            errorWriter = SequenceFile.createWriter(FileSystem.get(context.getConfiguration()),
                    context.getConfiguration(),
                    new Path(committer.getWorkPath(), getUniqueFile(context, ERRORS_PREFIX, "")),
                    EtlKey.class, ExceptionWritable.class);

            if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
                int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
                beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
            } else {
                beginTimeStamp = 0;
            }

            log.info("UNONG MultiEtlRecordWriter beginTimeStamp : " + beginTimeStamp);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            log.debug("UNONG MultiEtlRecordWriter close called");
            for (String w : dataWriters.keySet()) {
                log.info("UNONG MultiEtlRecordWriter close dataWritersKey :: " + w);
                dataWriters.get(w).close(context);
            }
            errorWriter.close();
        }

        @Override
        public void write(EtlKey key, Object val) throws IOException, InterruptedException {
            log.debug("UNONG MultiEtlRecordWriter write called");
//            key.put(new Text("object"), new ObjectWritable(val));

            if (val instanceof CamusWrapper<?>) {
                log.info("UNONG etlKey getTime :: " + key.getTime());

                if (key.getTime() < beginTimeStamp) {
                    // ((Mapper.Context)context).getCounter("total",
                    // "skip-old").increment(1);
                    committer.addOffset(key);
                } else {
                    if (!key.getTopic().equals(currentTopic)) {
                        for (RecordWriter<IEtlKey, CamusWrapper> writer : dataWriters
                                .values()) {
                            writer.close(context);
                        }
                        dataWriters.clear();
                        currentTopic = key.getTopic();
                    }

                    committer.addCounts(key);
                    CamusWrapper value = (CamusWrapper) val;
                    // TODO unong : 여기 val 값중 시간값으로 workingFileName 에 명시..
                    String workingFileName = getWorkingFileName(context, key);
                    log.info("UNONG write workingFileName : " + workingFileName + " :: " + value.toString());
                    if (!dataWriters.containsKey(workingFileName)) {
                        dataWriters.put(
                                workingFileName,
                                getDataRecordWriter(context, workingFileName, value));
                    }
                    dataWriters.get(workingFileName).write(key, value);
                }
            } else if (val instanceof ExceptionWritable) {
                committer.addOffset(key);
                System.err.println(key.toString());
                System.err.println(val.toString());
                errorWriter.append(key, (ExceptionWritable) val);
            }
        }
    }

    public class EtlMultiOutputCommitter extends FileOutputCommitter {
        Pattern workingFileMetadataPattern;

        HashMap<String, EtlCounts> counts = new HashMap<String, EtlCounts>();
        HashMap<String, EtlKey> offsets = new HashMap<String, EtlKey>();

        TaskAttemptContext context;
        private final RecordWriterProvider recordWriterProvider;


        public void addCounts(EtlKey key) throws IOException {
            log.debug("UNONG EtlMultiOutputCommitter addCounts called");

            String workingFileName = getWorkingFileName(context, key);
            if (!counts.containsKey(workingFileName))
                counts.put(workingFileName, new EtlCounts(key.getTopic(),
                        granularityMs));
            counts.get(workingFileName).incrementMonitorCount(key);
            addOffset(key);
        }

        public void addOffset(EtlKey key) {
            log.debug("UNONG EtlMultiOutputCommitter addOffset called");
            String topicPart = key.getTopic() + "-" + key.getLeaderId() + "-" + key.getPartition();
            offsets.put(topicPart, new EtlKey(key));
        }

        public EtlMultiOutputCommitter(Path outputPath, TaskAttemptContext context)
                throws IOException {
            super(outputPath, context);
            log.debug("UNONG EtlMultiOutputCommitter called");

            this.context = context;
            try {
                recordWriterProvider = getRecordWriterProviderClass(context).newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            workingFileMetadataPattern = Pattern.compile("data\\.([^\\.]+)\\.(\\d+)\\.(\\d+)\\.([^\\.]+)-m-\\d+" + recordWriterProvider.getFilenameExtension());
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            log.debug("UNONG EtlMultiOutputCommitter commitTask called");

        	ArrayList<Map<String,Object>> allCountObject = new ArrayList<Map<String,Object>>();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (isRunMoveData(context)) {
                Path workPath = super.getWorkPath();
                log.info("UNONG commitTask workPath :: " + workPath.toString());
                Path baseOutDir = getDestinationPath(context);
                log.info("UNONG commitTask baseOutDir :: " + baseOutDir.toString());
                for (FileStatus f : fs.listStatus(workPath)) {
                    String file = f.getPath().getName();
                    log.info("UNONG commitTask file :: " + file);
                    if (file.startsWith("data")) {
                        String workingFileName = file.substring(0, file.lastIndexOf("-m"));
                        log.info("UNONG commitTask workingFileName :: " + workingFileName);
                        EtlCounts count = counts.get(workingFileName);
                        log.info("UNONG commitTask EtlCounts topic :: " + count.getTopic() + ", " + count.getLastKey().toString());

                        String partitionedFile = getPartitionedPath(context, file,
                                count.getEventCount(), count.getLastKey().getOffset());
                        log.info("UNONG commitTask partitionedFile :: " + partitionedFile);

                        Path dest = new Path(baseOutDir, partitionedFile);
                        log.info("UNONG commitTask dest :: " + dest.toString());

                        if (!fs.exists(dest.getParent())) {
                                fs.mkdirs(dest.getParent());
                            }

                        fs.rename(f.getPath(), dest);

                        if (isRunTrackingPost(context)) {
                                count.writeCountsToHDFS(allCountObject, fs, new Path(workPath, COUNTS_PREFIX + "."
                                        + dest.getName().replace(recordWriterProvider.getFilenameExtension(), "")));
                        }
                    }
                }

                Path tempPath = new Path(workPath, "counts." + context.getConfiguration().get("mapred.task.id"));
                log.info("UNONG commitTask tempPath :: " + tempPath.toString());
                OutputStream outputStream = new BufferedOutputStream(fs.create(tempPath));
                ObjectMapper mapper= new ObjectMapper();
                log.info("Writing counts to : " + tempPath.toString());
                long time = System.currentTimeMillis();
                mapper.writeValue(outputStream, allCountObject);
                log.debug("Time taken : " + (System.currentTimeMillis() - time)/1000);
            }

            SequenceFile.Writer offsetWriter = SequenceFile.createWriter(fs,
                    context.getConfiguration(),
                    new Path(super.getWorkPath(), getUniqueFile(context, OFFSET_PREFIX, "")),
                    EtlKey.class, NullWritable.class);
            for (String s : offsets.keySet()) {
                log.info("UNONG commitTask offsets :: " + s + " :: " + offsets.get(s).toString());
                offsetWriter.append(offsets.get(s), NullWritable.get());
            }
            offsetWriter.close();
            super.commitTask(context);
        }

        public String getPartitionedPath(JobContext context, String file, int count, long offset) throws IOException {
            log.debug("UNONG EtlMultiOutputCommitter getPartitionedPath called");

            Matcher m = workingFileMetadataPattern.matcher(file);
            if(! m.find()) {
                throw new IOException("Could not extract metadata from working filename '" + file + "'");
            }
            String topic = m.group(1);
            String leaderId = m.group(2);
            String partition = m.group(3);
            String encodedPartition = m.group(4);

            String partitionedPath =
                        getPartitioner(context, topic).generatePartitionedPath(context, topic, Integer.parseInt(leaderId),
                                Integer.parseInt(partition), encodedPartition);

            return partitionedPath +
                        "/" + topic + "." + leaderId + "." + partition +
                        "." + count+
                        "." + offset + recordWriterProvider.getFilenameExtension();
        }
    }
}
