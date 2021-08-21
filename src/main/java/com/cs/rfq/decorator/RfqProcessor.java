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
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.Materialized;
//import org.apache.kafka.streams.kstream.Produced;
//import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Date;
import java.util.*;

import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        this.trades = new TradeDataLoader().loadTrades(this.session, "src/test/resources/trades/trades.json");

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

        JavaPairDStream<String, String> tuples = stream.mapToPair(record ->
                new Tuple2<>(record.key(), record.value()));

        JavaDStream<String> jsonStream = stream.map(elt -> elt.value());

        jsonStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                //stringJavaRDD.collect().forEach(System.out::println);
                stringJavaRDD.collect().forEach(json -> processRfq(Rfq.fromJson(json)));
            }
        });


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
        listOfFields.add(fields[1]);
        StructType structType = DataTypes.createStructType(listOfFields);

        Dataset<Row> metaSet = session.createDataFrame(list, structType);
        //metaSet.
        metaSet.show();
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
