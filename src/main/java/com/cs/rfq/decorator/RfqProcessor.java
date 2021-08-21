package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalTradesWithEntityExtractor;
import com.cs.rfq.decorator.extractors.VolumeTradedWithEntityYTDExtractor;
import com.cs.rfq.decorator.extractors.InstrumentLiquidityExtractor;
import com.cs.rfq.decorator.extractors.AverageTradedPriceExtractor;
import com.cs.rfq.decorator.extractors.TradeSideBiasExtractor;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Date;
import java.util.*;
import java.util.function.Consumer;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class RfqProcessor implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private transient final SparkSession session;

    private transient final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        this.trades = new TradeDataLoader().loadTrades(this.session, "src/test/resources/trades/trades.json");


        System.out.println("======= TRADES NULL ======");
        System.out.println(trades);
        System.out.println();

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new AverageTradedPriceExtractor());
        extractors.add(new TradeSideBiasExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        //JavaDStream<String> lines = streamingContext.socketTextStream("localhost",9000);

//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);




        Collection<String> topics = Arrays.asList("streams-plaintext-input");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

//        JavaPairDStream<String, String> tuples = stream.mapToPair(record ->
//                new Tuple2<>(record.key(), record.value()));

        JavaDStream<String> jsonStream = stream.map(elt -> elt.value());

        // value : {ad: 123, }

        // createRfq(val) -> RFQ

        // processRfq(RFQ) -> Map<RfqMetaDataFieldName, Object>

        // jsonString(Map<RfqMetaDataFieldName, Object>) -> JsonString

        // publishToKafka(JsonString)

        Map<String, Object> kafkaParamsOut = new HashMap<>();
        kafkaParamsOut.put("bootstrap.servers", "localhost:9092");
        kafkaParamsOut.put("key.serializer", StringSerializer.class);
        kafkaParamsOut.put("value.serializer", StringSerializer.class);
        kafkaParamsOut.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaParamsOut.put(ProducerConfig.CLIENT_ID_CONFIG, "RFQMetaDataProducer");
        //        props.put("linger.ms", 5);
        //        props.put("retries", "3");

//        jsonStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringRDD) throws Exception {
//                stringRDD.collect().forEach(new Consumer<String>() {
//                    @Override
//                    public void accept(String s) {
//                        publishToKafka(s,
//                                1L,
//                                "metadata_json2",
//                                kafkaParamsOut);
//                    }
//                });
//            }
//        });
//
//

        // JavaDStream<String> jsonStream;


        System.out.println("------- TRADES --------");
        System.out.println(trades);
        System.out.println();
        JavaDStream<Map<RfqMetadataFieldNames, Object>> metaStream = jsonStream
                .map(new Function<String, Map<RfqMetadataFieldNames, Object>>() {
                    @Override
                    public Map<RfqMetadataFieldNames, Object> call(String s) throws Exception {
                        Map<RfqMetadataFieldNames, Object> meta = fetchMeta(Rfq.fromJson(s));
                        log.info("Fetching an extractor's metadata", meta);
                        return meta;
                    }
                });




        metaStream.foreachRDD(new VoidFunction<JavaRDD<Map<RfqMetadataFieldNames, Object>>>() {
            @Override
            public void call(JavaRDD<Map<RfqMetadataFieldNames, Object>> metaRDD) throws Exception {
                metaRDD.collect().forEach(new Consumer<Map<RfqMetadataFieldNames, Object>>() {
                    @Override
                    public void accept(Map<RfqMetadataFieldNames, Object> metaData) {
//                        publishToKafka(new GsonBuilder().setPrettyPrinting().create().toJson(metaData),
//                                1L,
//                                "metadata_json3",
//                                kafkaParamsOut);

                    }
                });
            }
        });






        //String outputTopic = "producerTopic";
        //publishToKafka(key, wordCountMap.get(key), outputTopic);




//        jsonStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                //stringJavaRDD.collect().forEach(System.out::println);
//                stringJavaRDD.collect().forEach(json -> processRfq(Rfq.fromJson(json)));
//            }
//        });
//


        //session.createDataFrame(ss.)


        //KafkaProducer<String, String> producer =




//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> textLines = builder.stream("streams-plaintext-input");
//        KTable<String, Long> wordCounts = textLines
//                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
//                .groupBy((key, word) -> word)
//                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//        //wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), props);
//        System.out.println("====== HERE======");
//        streams.start();

        //org.apache.kafka.common.serialization.Serdes$StringSerde;


        //TODO: convert each incoming line to a Rfq object and call processRfq method with it

//        JavaDStream<Rfq> rfqs = lines.map(json -> Rfq.fromJson(json));
//
//        rfqs.foreachRDD(rdd -> {
//            rdd.collect().forEach(rfq -> processRfq(rfq));
//        });

        //TODO: start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }




    public static void publishToKafka(String word, Long count, String topic, Map<String, Object> props) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        long time = System.currentTimeMillis();
        try {
            //ObjectMapper mapper = new ObjectMapper();
            //String jsonInString = mapper.writeValueAsString(word + " " + count);
            String event = word;
            //log.info("Message to send to kafka : {}", event);
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, event);

            RecordMetadata metadata = producer.send(record)
                    .get();

            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);

            log.info("Event : " + event + " published successfully to kafka!!");
        } catch (Exception e) {
            log.error("Problem while publishing the event to kafka : " + e.getMessage());
        }
        producer.flush();
        producer.close();
    }



    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        Map<RfqMetadataFieldNames, Object> extractorMetadata;
        //TODO: get metadata from each of the extractors
        for (RfqMetadataExtractor extractor : extractors) {
            extractorMetadata = extractor.extractMetaData(rfq, session, trades);
            extractorMetadata.forEach(metadata::putIfAbsent);
        }

        //TODO: add RFQ id tp meta data

//        JavaRDD row
//        StructSchema
        //session.createDataFrame()

        writeToKafka(metadata, rfq);


        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
    }


    public Map<RfqMetadataFieldNames, Object> fetchMeta(Rfq rfq) {
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //metadata.put()
        System.out.println("======== TRADES fetch meta =======");
        System.out.println(trades);
        System.out.println();
        Map<RfqMetadataFieldNames, Object> extractorMetadata;

        extractorMetadata = extractors.get(3).extractMetaData(rfq, session, trades);

        return extractorMetadata;

        //TODO: get metadata from each of the extractors
//        for (RfqMetadataExtractor extractor : extractors) {
//            extractorMetadata = extractor.extractMetaData(rfq, session, trades);
//            extractorMetadata.forEach(metadata::putIfAbsent);
//        }
//
//        return metadata;
    }





    private void writeToKafka(Map<RfqMetadataFieldNames, Object> metaData, Rfq rfq) {
        // Create Struct Schema for output json
        StructField[] fields = new StructField[2];

        fields[0] = new StructField("RfqId", type(rfq.getId()), false, Metadata.empty());
        fields[1] = new StructField("VolTradedLastYear",
                type(metaData.get(RfqMetadataFieldNames.volumeTradedYearToDate)),
                false,
                Metadata.empty());

        StructType schema = new StructType(fields);
        //RfqMetadataFieldNames.volumeTradedYearToDate
//        StructType schema = new StructType(new StructField[] {
//                new StructField("RfqId", StringType, false,  Metadata.empty()),
//                new StructField("EntityId", LongType, false,  Metadata.empty()),
//                new StructField("SecurityID", StringType, false, Metadata.empty()),
//                new StructField("LastQty", LongType, false, Metadata.empty()),
//                new StructField("LastPx", DoubleType, false, Metadata.empty()),
//                new StructField("TradeDate", DateType, false, Metadata.empty()),
//                new StructField("Currency", StringType, false, Metadata.empty()),
//                new StructField("Side", IntegerType, false, Metadata.empty()),
//        });


        //JavaRDD<Row> rows =
        //session.
        List<Row> list =new ArrayList<>();
        String s = new GsonBuilder().setPrettyPrinting().create().toJson(metaData);
        list.add(RowFactory.create(s));
        //list.add(RowFactory.create("two"));

        //session.createDataFrame()

        List<StructField> listOfFields = new ArrayList<>();
        listOfFields.add(fields[0]);
        //listOfFields.add(fields[1]);
        StructType structType = DataTypes.createStructType(listOfFields);

        Dataset<Row> metaSet = session.createDataFrame(list, structType);

        //metaSet.writeStream()

//        try {
//            metaSet.writeStream()
//                    .format("kafka")
//                    .outputMode("append")
//                    .option("kafka.bootstrap.servers", "localhost:9092")
//                    .option("topic", "streams-wordcount-output")
//                    .start()
//                    .awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }

        //metaSet.
        metaSet.show();
        metaSet.writeStream();
        //Dataset<Row> trades = session.read().schema(schema).json(path)
    }


    // probably create some sort of Factory in the future
    private DataType type(Object entry) {
        if (entry instanceof String)
            return StringType;
        else if (entry instanceof Integer)
            return IntegerType;
        else if (entry instanceof Long)
            return LongType;
        else if (entry instanceof Double)
            return DoubleType;
        else if (entry instanceof Date)
            return DateType;
        else
            return null; // should not happen
    }


}
