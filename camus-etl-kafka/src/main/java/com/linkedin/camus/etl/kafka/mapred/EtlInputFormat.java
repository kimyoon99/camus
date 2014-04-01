package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Input format for a Kafka pull job.
 */
public class EtlInputFormat extends InputFormat<EtlKey, CamusWrapper> {

	public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
	public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

	public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

	public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
	public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

	public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
	public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
	public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

	public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
	public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
	public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

	private static final Logger log = Logger.getLogger(EtlInputFormat.class);

//    @Override
//    public org.apache.hadoop.mapred.RecordReader<EtlKey, CamusWrapper> getRecordReader(org.apache.hadoop.mapred.InputSplit inputSplit, org.apache.hadoop.mapred.JobConf entries, org.apache.hadoop.mapred.Reporter reporter) throws IOException {
//        log.debug("UNONG getRecordReader called");
//
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }

    @Override
    public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        log.error("UNONG createRecordReader called split length :: " + split.getLength());

        if(split.getLocations() != null ) {
            log.error("UNONG createRecordReader split location count :: " + split.getLocations().length);
            for(String lo : split.getLocations()) {
                log.error("UNONG createRecordReader :: " + lo);
            }
        } else {
            log.error("UNONG split no locations ");
        }

        return new EtlRecordReader(split, context);
    }

	/**
	 * Gets the metadata from Kafka
	 * 
	 * @param context
	 * @return
	 */
	public List<TopicMetadata> getKafkaMetadata(JobContext context) {
        log.debug("UNONG getKafkaMetadata called");

		ArrayList<String> metaRequestTopics = new ArrayList<String>();
		CamusJob.startTiming("kafkaSetupTime");
		String brokerString = CamusJob.getKafkaBrokers(context);
                List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
		Collections.shuffle(brokers);
		boolean fetchMetaDataSucceeded = false;
		int i = 0;
		List<TopicMetadata> topicMetadataList = null;
		Exception savedException = null;
		while (i < brokers.size() && !fetchMetaDataSucceeded) {
			SimpleConsumer consumer = createConsumer(context, brokers.get(i));
			log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s",
			brokers.get(i), consumer.clientId(), metaRequestTopics.size(), metaRequestTopics));
			try {
				topicMetadataList = consumer.send(new TopicMetadataRequest(metaRequestTopics)).topicsMetadata();
				fetchMetaDataSucceeded = true;
			} catch (Exception e) {
				savedException = e;
				log.warn(String.format("Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed",
					consumer.clientId(), metaRequestTopics, brokers.get(i)), e);
			} finally {
				consumer.close();
				i++;
			}
		}
		if (!fetchMetaDataSucceeded) {
			throw new RuntimeException("Failed to obtain metadata!", savedException);
		}
		CamusJob.stopTiming("kafkaSetupTime");
		return topicMetadataList;
	}
 
	private SimpleConsumer createConsumer(JobContext context, String broker) {
        log.debug("UNONG createConsumer called");

		String[] hostPort = broker.split(":");
		SimpleConsumer consumer = new SimpleConsumer(
			hostPort[0],
			Integer.valueOf(hostPort[1]),
			CamusJob.getKafkaTimeoutValue(context),
			CamusJob.getKafkaBufferSize(context),
			CamusJob.getKafkaClientName(context));
		return consumer;
	}

	/**
	 * Gets the latest offsets and create the requests as needed
	 * 
	 * @param context
	 * @param offsetRequestInfo
	 * @return
	 */
	public ArrayList<EtlRequest> fetchLatestOffsetAndCreateEtlRequests(
			JobContext context,
			HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {
        log.info("UNONG fetchLatestOffsetAndCreateEtlRequests called :: " + offsetRequestInfo.size());

		ArrayList<EtlRequest> finalRequests = new ArrayList<EtlRequest>();
		for (LeaderInfo leader : offsetRequestInfo.keySet()) {
            log.info("UNONG LeaderInfo " + leader.getLeaderId() + " :: " + leader.getUri().toString() + " :: " + leader.toString());

			SimpleConsumer consumer = new SimpleConsumer(leader.getUri()
					.getHost(), leader.getUri().getPort(),
					CamusJob.getKafkaTimeoutValue(context),
					CamusJob.getKafkaBufferSize(context),
					CamusJob.getKafkaClientName(context));

            log.info("SimpleConsumer : " + consumer.toString());

			// Latest Offset
			PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
					kafka.api.OffsetRequest.LatestTime(), 1);
			// Earliest Offset
			PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
					kafka.api.OffsetRequest.EarliestTime(), 1);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
					.get(leader);
			for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                log.info("UNONG topic & Partition :: " + topicAndPartition.topic() + " , " + topicAndPartition.partition() + " , " + topicAndPartition.toString());
                log.info("UNONG partitionLatestOffsetRequestInfo :: " + partitionLatestOffsetRequestInfo.productPrefix() + ", " + partitionLatestOffsetRequestInfo.time() + " , " + partitionLatestOffsetRequestInfo.maxNumOffsets() + " , " + partitionLatestOffsetRequestInfo.toString());
                log.info("UNONG partitionEarliestOffsetRequestInfo :: " + partitionEarliestOffsetRequestInfo.productPrefix() + ", " + partitionEarliestOffsetRequestInfo.time() + " , " + partitionEarliestOffsetRequestInfo.maxNumOffsets() + " , " + partitionEarliestOffsetRequestInfo.toString());
				latestOffsetInfo.put(topicAndPartition,
						partitionLatestOffsetRequestInfo);
				earliestOffsetInfo.put(topicAndPartition,
						partitionEarliestOffsetRequestInfo);
			}

			OffsetResponse latestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(), CamusJob
									.getKafkaClientName(context)));

			OffsetResponse earliestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(earliestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(), CamusJob
									.getKafkaClientName(context)));
			consumer.close();
			for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                log.info("UNONG after consumer close " + topicAndPartition.topic() + "::" + topicAndPartition.partition());
				long latestOffset = latestOffsetResponse.offsets(
						topicAndPartition.topic(),
						topicAndPartition.partition())[0];
                log.info("UNONG after consumer close latestOffset " + latestOffset);
				long earliestOffset = earliestOffsetResponse.offsets(
						topicAndPartition.topic(),
						topicAndPartition.partition())[0];
                log.info("UNONG after consumer close earliestOffset " + earliestOffset);
				EtlRequest etlRequest = new EtlRequest(context,
						topicAndPartition.topic(), Integer.toString(leader
								.getLeaderId()), topicAndPartition.partition(),
						leader.getUri());
                log.info("UNONG etlRequest " + etlRequest.getTopic() + ":" + etlRequest.getOffset() + ":" + etlRequest.getEarliestOffset() + ":" + etlRequest.getLastOffset() + "::" + etlRequest.toString());

				etlRequest.setLatestOffset(latestOffset);
				etlRequest.setEarliestOffset(earliestOffset);
				finalRequests.add(etlRequest);
			}
		}
		return finalRequests;
	}

	public String createTopicRegEx(HashSet<String> topicsSet) {
        log.debug("UNONG createTopicRegEx called");

		String regex = "";
		StringBuilder stringbuilder = new StringBuilder();
		for (String whiteList : topicsSet) {
			stringbuilder.append(whiteList);
			stringbuilder.append("|");
		}
		regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1)
				+ ")";
		Pattern.compile(regex);
		return regex;
	}

	public List<TopicMetadata> filterWhitelistTopics(
			List<TopicMetadata> topicMetadataList,
			HashSet<String> whiteListTopics) {
        log.debug("UNONG filterWhitelistTopics called");

		ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
		String regex = createTopicRegEx(whiteListTopics);
		for (TopicMetadata topicMetadata : topicMetadataList) {
			if (Pattern.matches(regex, topicMetadata.topic())) {
				filteredTopics.add(topicMetadata);
			} else {
				log.info("Discrading topic : " + topicMetadata.topic());
			}
		}
		return filteredTopics;
	}

//    @Override
//    public org.apache.hadoop.mapred.InputSplit[] getSplits(org.apache.hadoop.mapred.JobConf entries, int i) throws IOException {
//        log.info("UNONG older getSplits called");
//        return new org.apache.hadoop.mapred.InputSplit[0];
//    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        log.debug("UNONG getSplits called");

		CamusJob.startTiming("getSplits");
		ArrayList<EtlRequest> finalRequests;
		HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo = new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();
		try {

			// Get Metadata for all topics
			List<TopicMetadata> topicMetadataList = getKafkaMetadata(context);
            for(TopicMetadata topicMetadata : topicMetadataList) {
                log.info("UNONG topic" + topicMetadata.topic() + " metadata errcode : " + topicMetadata.errorCode() + " sizeInByte" + topicMetadata.sizeInBytes());
                for(PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                    log.info("UNONG partition : " + partitionMetadata.partitionId() + " : leaderhost : " + partitionMetadata.leader().host() + " sizeInByte" + partitionMetadata.sizeInBytes());
                }
            }

			// Filter any white list topics
			HashSet<String> whiteListTopics = new HashSet<String>(
					Arrays.asList(getKafkaWhitelistTopic(context)));
			if (!whiteListTopics.isEmpty()) {
				topicMetadataList = filterWhitelistTopics(topicMetadataList,
						whiteListTopics);
			}

			// Filter all blacklist topics
			HashSet<String> blackListTopics = new HashSet<String>(
					Arrays.asList(getKafkaBlacklistTopic(context)));
			String regex = "";
			if (!blackListTopics.isEmpty()) {
				regex = createTopicRegEx(blackListTopics);
			}

			for (TopicMetadata topicMetadata : topicMetadataList) {
				if (Pattern.matches(regex, topicMetadata.topic())) {
					log.info("Discarding topic (blacklisted): "
							+ topicMetadata.topic());
				} else if (!createMessageDecoder(context, topicMetadata.topic())) {
					log.info("Discarding topic (Decoder generation failed) : "
							+ topicMetadata.topic());
				} else {
					for (PartitionMetadata partitionMetadata : topicMetadata
							.partitionsMetadata()) {
						if (partitionMetadata.errorCode() != ErrorMapping
								.NoError()) {
							log.info("Skipping the creation of ETL request for Topic : "
									+ topicMetadata.topic()
									+ " and Partition : "
									+ partitionMetadata.partitionId()
									+ " Exception : "
									+ ErrorMapping
											.exceptionFor(partitionMetadata
													.errorCode()));
							continue;
						} else {

							LeaderInfo leader = new LeaderInfo(new URI("tcp://"
									+ partitionMetadata.leader()
											.getConnectionString()),
									partitionMetadata.leader().id());
							if (offsetRequestInfo.containsKey(leader)) {
								ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
										.get(leader);
								topicAndPartitions.add(new TopicAndPartition(
										topicMetadata.topic(),
										partitionMetadata.partitionId()));
								offsetRequestInfo.put(leader,
										topicAndPartitions);
							} else {
								ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<TopicAndPartition>();
								topicAndPartitions.add(new TopicAndPartition(
										topicMetadata.topic(),
										partitionMetadata.partitionId()));
								offsetRequestInfo.put(leader,
										topicAndPartitions);
							}

						}
					}
				}
			}
		} catch (Exception e) {
			log.error(
					"Unable to pull requests from Kafka brokers. Exiting the program",
					e);
			return null;
		}
		// Get the latest offsets and generate the EtlRequests
		finalRequests = fetchLatestOffsetAndCreateEtlRequests(context,
				offsetRequestInfo);

		Collections.sort(finalRequests, new Comparator<EtlRequest>() {
			public int compare(EtlRequest r1, EtlRequest r2) {
				return r1.getTopic().compareTo(r2.getTopic());
			}
		});

		writeRequests(finalRequests, context);
		Map<EtlRequest, EtlKey> offsetKeys = getPreviousOffsets(
				FileInputFormat.getInputPaths(context), context);
		Set<String> moveLatest = getMoveToLatestTopicsSet(context);
		for (EtlRequest request : finalRequests) {
			if (moveLatest.contains(request.getTopic())
					|| moveLatest.contains("all")) {
				offsetKeys.put(
						request,
						new EtlKey(request.getTopic(), request.getLeaderId(),
								request.getPartition(), 0, request
										.getLastOffset()));
			}

			EtlKey key = offsetKeys.get(request);

			if (key != null) {
				request.setOffset(key.getOffset());
			}

            log.debug("UNONG request : " + request.getTopic() + "(" + request.getEarliestOffset() + "~" + request.getLastOffset() + ")-" + request.getOffset() + "::" + request.toString());

			if (request.getEarliestOffset() > request.getOffset()
					|| request.getOffset() > request.getLastOffset()) {
				if(request.getEarliestOffset() > request.getOffset())
				{
					log.error("The earliest offset was found to be more than the current offset");
					log.error("Moving to the earliest offset available");
				}
				else
				{
					log.error("The current offset was found to be more than the latest offset");
					log.error("Moving to the earliest offset available");
				}
				request.setOffset(request.getEarliestOffset());
				offsetKeys.put(
						request,
						new EtlKey(request.getTopic(), request.getLeaderId(),
								request.getPartition(), 0, request
										.getLastOffset()));
			}
            log.debug("UNONG request : " + request.getTopic() + "(" + request.getEarliestOffset() + "~" + request.getLastOffset() + ")-" + request.getOffset() + "::" + request.toString());
		}

		writePrevious(offsetKeys.values(), context);

		CamusJob.stopTiming("getSplits");
		CamusJob.startTiming("hadoop");
		CamusJob.setTime("hadoop_start");

        List<InputSplit> kafkaETLSplits = allocateWork(finalRequests, context);

        log.info("UNONG kafkaETLSplits size : " + kafkaETLSplits.size() + " finished getSplits");
		return kafkaETLSplits;
	}

	private Set<String> getMoveToLatestTopicsSet(JobContext context) {
        log.debug("UNONG getMoveToLatestTopicsSet called");

		Set<String> topics = new HashSet<String>();

		String[] arr = getMoveToLatestTopics(context);

		if (arr != null) {
			for (String topic : arr) {
				topics.add(topic);
			}
		}

		return topics;
	}

	private boolean createMessageDecoder(JobContext context, String topic) {
        log.debug("UNONG createMessageDecoder called");

		try {
			MessageDecoderFactory.createMessageDecoder(context, topic);
			return true;
		} catch (Exception e) {
            log.error(e);

			return false;
		}
	}

	private List<InputSplit> allocateWork(List<EtlRequest> requests,
			JobContext context) throws IOException {
        log.info("UNONG allocateWork called : requestSize = " + requests.size());

		int numTasks = context.getConfiguration()
				.getInt("mapred.map.tasks", 30);
		// Reverse sort by size
		Collections.sort(requests, new Comparator<EtlRequest>() {
			@Override
			public int compare(EtlRequest o1, EtlRequest o2) {
				if (o2.estimateDataSize() == o1.estimateDataSize()) {
					return 0;
				}
				if (o2.estimateDataSize() < o1.estimateDataSize()) {
					return -1;
				} else {
					return 1;
				}
			}
		});

		List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

		for (int i = 0; i < numTasks; i++) {
			EtlSplit split = new EtlSplit();

			if (requests.size() > 0) {
                log.debug("UNONG split request : " + requests.get(0).toString());
				split.addRequest(requests.get(0));
				kafkaETLSplits.add(split);
				requests.remove(0);
			}
		}

		for (EtlRequest r : requests) {
			getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
		}

		return kafkaETLSplits;
	}

	private EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits)
			throws IOException {
        log.debug("UNONG getSmallestMultiSplit called");

		EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

		for (int i = 1; i < kafkaETLSplits.size(); i++) {
			EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
			if ((smallest.getLength() == challenger.getLength() && smallest
					.getNumRequests() > challenger.getNumRequests())
					|| smallest.getLength() > challenger.getLength()) {
				smallest = challenger;
			}
		}

		return smallest;
	}

	private void writePrevious(Collection<EtlKey> missedKeys, JobContext context)
			throws IOException {
        log.debug("UNONG writePrevious called");

		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path output = FileOutputFormat.getOutputPath(context);

		if (fs.exists(output)) {
			fs.mkdirs(output);
		}

		output = new Path(output, EtlMultiOutputFormat.OFFSET_PREFIX
				+ "-previous");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
				context.getConfiguration(), output, EtlKey.class,
				NullWritable.class);

		for (EtlKey key : missedKeys) {
			writer.append(key, NullWritable.get());
		}

		writer.close();
	}

	private void writeRequests(List<EtlRequest> requests, JobContext context)
			throws IOException {
        log.debug("UNONG writeRequests called");

		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path output = FileOutputFormat.getOutputPath(context);

        log.info("UNONG output URI :" + output.toUri() + " :: " + output.toString());
		if (fs.exists(output)) {
			fs.mkdirs(output);
		}

		output = new Path(output, EtlMultiOutputFormat.REQUESTS_FILE);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
				context.getConfiguration(), output, EtlRequest.class,
				NullWritable.class);

		for (EtlRequest r : requests) {
			writer.append(r, NullWritable.get());
		}
		writer.close();
	}

	private Map<EtlRequest, EtlKey> getPreviousOffsets(Path[] inputs,
			JobContext context) throws IOException {
        log.debug("UNONG getPreviousOffsets called");

		Map<EtlRequest, EtlKey> offsetKeysMap = new HashMap<EtlRequest, EtlKey>();
		for (Path input : inputs) {
            log.info("UNONG input Path : " + input.toString());
			FileSystem fs = input.getFileSystem(context.getConfiguration());
			for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
				log.info("previous offset file:" + f.getPath().toString());
//				SequenceFile.Reader reader = new SequenceFile.Reader(fs,
//						f.getPath(), context.getConfiguration());

                SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(f.getPath()));
				EtlKey key = new EtlKey();
				while (reader.next(key, NullWritable.get())) {
					EtlRequest request = new EtlRequest(context,
							key.getTopic(), key.getLeaderId(),
							key.getPartition());
                    log.debug("UNONG previous offset request : " + request.toString());
					if (offsetKeysMap.containsKey(request)) {

						EtlKey oldKey = offsetKeysMap.get(request);
						if (oldKey.getOffset() < key.getOffset()) {
							offsetKeysMap.put(request, key);
						}
					} else {
						offsetKeysMap.put(request, key);
					}
					key = new EtlKey();
				}
				reader.close();
			}
		}
		return offsetKeysMap;
	}

	public static void setMoveToLatestTopics(JobContext job, String val) {
        log.debug("UNONG setMoveToLatestTopics called");
		job.getConfiguration().set(KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
	}

	public static String[] getMoveToLatestTopics(JobContext job) {
        log.debug("UNONG getMoveToLatestTopics called");
		return job.getConfiguration()
				.getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
	}

	public static void setKafkaClientBufferSize(JobContext job, int val) {
        log.debug("UNONG setKafkaClientBufferSize called");
		job.getConfiguration().setInt(KAFKA_CLIENT_BUFFER_SIZE, val);
	}

	public static int getKafkaClientBufferSize(JobContext job) {
        log.debug("UNONG getKafkaClientBufferSize called");
		return job.getConfiguration().getInt(KAFKA_CLIENT_BUFFER_SIZE,
				2 * 1024 * 1024);
	}

	public static void setKafkaClientTimeout(JobContext job, int val) {
        log.debug("UNONG setKafkaClientTimeout called");
		job.getConfiguration().setInt(KAFKA_CLIENT_SO_TIMEOUT, val);
	}

	public static int getKafkaClientTimeout(JobContext job) {
        log.debug("UNONG getKafkaClientTimeout called");
		return job.getConfiguration().getInt(KAFKA_CLIENT_SO_TIMEOUT, 60000);
	}

	public static void setKafkaMaxPullHrs(JobContext job, int val) {
        log.debug("UNONG setKafkaMaxPullHrs called");
		job.getConfiguration().setInt(KAFKA_MAX_PULL_HRS, val);
	}

	public static int getKafkaMaxPullHrs(JobContext job) {
        log.debug("UNONG getKafkaMaxPullHrs called");
		return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
	}

	public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val) {
        log.debug("UNONG setKafkaMaxPullMinutesPerTask called");
		job.getConfiguration().setInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
	}

	public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
        log.debug("UNONG getKafkaMaxPullMinutesPerTask called");
		return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK,
				-1);
	}

	public static void setKafkaMaxHistoricalDays(JobContext job, int val) {
        log.debug("UNONG setKafkaMaxHistoricalDays called");
		job.getConfiguration().setInt(KAFKA_MAX_HISTORICAL_DAYS, val);
	}

	public static int getKafkaMaxHistoricalDays(JobContext job) {
        log.debug("UNONG getKafkaMaxHistoricalDays called");
		return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
	}

	public static void setKafkaBlacklistTopic(JobContext job, String val) {
        log.debug("UNONG setKafkaBlacklistTopic called");
		job.getConfiguration().set(KAFKA_BLACKLIST_TOPIC, val);
	}

	public static String[] getKafkaBlacklistTopic(JobContext job) {
        log.debug("UNONG getKafkaBlacklistTopic called");
		if (job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC) != null
				&& !job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC).isEmpty()) {
			return job.getConfiguration().getStrings(KAFKA_BLACKLIST_TOPIC);
		} else {
			return new String[] {};
		}
	}

	public static void setKafkaWhitelistTopic(JobContext job, String val) {
        log.debug("UNONG setKafkaWhitelistTopic called");
		job.getConfiguration().set(KAFKA_WHITELIST_TOPIC, val);
	}

	public static String[] getKafkaWhitelistTopic(JobContext job) {
        log.debug("UNONG getKafkaWhitelistTopic called");
		if (job.getConfiguration().get(KAFKA_WHITELIST_TOPIC) != null
				&& !job.getConfiguration().get(KAFKA_WHITELIST_TOPIC).isEmpty()) {
			return job.getConfiguration().getStrings(KAFKA_WHITELIST_TOPIC);
		} else {
			return new String[] {};
		}
	}

	public static void setEtlIgnoreSchemaErrors(JobContext job, boolean val) {
        log.debug("UNONG setEtlIgnoreSchemaErrors called");
		job.getConfiguration().setBoolean(ETL_IGNORE_SCHEMA_ERRORS, val);
	}

	public static boolean getEtlIgnoreSchemaErrors(JobContext job) {
        log.debug("UNONG getEtlIgnoreSchemaErrors called");
		return job.getConfiguration().getBoolean(ETL_IGNORE_SCHEMA_ERRORS,
				false);
	}

	public static void setEtlAuditIgnoreServiceTopicList(JobContext job,
			String topics) {
        log.debug("UNONG setEtlAuditIgnoreServiceTopicList called");
		job.getConfiguration().set(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, topics);
	}

	public static String[] getEtlAuditIgnoreServiceTopicList(JobContext job) {
        log.debug("UNONG getEtlAuditIgnoreServiceTopicList called");
		return job.getConfiguration().getStrings(
				ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, "");
	}

	public static void setMessageDecoderClass(JobContext job,
			Class<MessageDecoder> cls) {
        log.debug("UNONG setMessageDecoderClass called");
		job.getConfiguration().setClass(CAMUS_MESSAGE_DECODER_CLASS, cls,
				MessageDecoder.class);
	}

	public static Class<MessageDecoder> getMessageDecoderClass(JobContext job) {
        log.debug("UNONG getMessageDecoderClass called");
		return (Class<MessageDecoder>) job.getConfiguration().getClass(
				CAMUS_MESSAGE_DECODER_CLASS, KafkaAvroMessageDecoder.class);
	}




    private class OffsetFileFilter implements PathFilter {

		@Override
		public boolean accept(Path arg0) {
            log.info("UNONG OffsetFileFilter.accept called : " + arg0.toString());
			return arg0.getName()
					.startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
		}
	}
}
