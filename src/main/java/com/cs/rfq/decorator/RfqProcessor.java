package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;

import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Serializable;

import java.util.*;
import java.util.function.Consumer;

public class RfqProcessor implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private transient final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();


    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext, String tradeHistoryPath) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives

        this.trades = new TradeDataLoader().loadTrades(this.session,
                tradeHistoryPath);


        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new RfqIdExtractor());
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new AverageTradedPriceExtractor());
        extractors.add(new TradeSideBiasExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        //JavaDStream<String> lines = streamingContext.socketTextStream("localhost",9000);


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("rfq-input");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        JavaDStream<String> jsonStream = stream.map(elt -> elt.value());


        Map<String, Object> kafkaParamsOut = new HashMap<>();
        kafkaParamsOut.put("bootstrap.servers", "localhost:9092");
        kafkaParamsOut.put("key.serializer", StringSerializer.class);
        kafkaParamsOut.put("value.serializer", StringSerializer.class);
        kafkaParamsOut.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaParamsOut.put(ProducerConfig.CLIENT_ID_CONFIG, "RFQMetaDataProducer");
        //        kafkaParamsOut.put("linger.ms", 5);
        //        kafkaParamsOut.put("retries", "3");


        // TODO: will turn into lambda
        jsonStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.collect().forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        publishToKafka(new GsonBuilder().setPrettyPrinting().create().toJson(fetchMeta(Rfq.fromJson(s))),
                                "rfq-metadata",
                                kafkaParamsOut);
                    }
                });
            }
        });


        //TODO: convert each incoming line to a Rfq object and call processRfq method with it

        //TODO: start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }




    public static void publishToKafka(String json, String topic, Map<String, Object> props) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        long time = System.currentTimeMillis();
        try {
            //log.info("Message to send to kafka : {}", event);
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, json);

            RecordMetadata metadata = producer.send(record)
                    .get();

            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);

            log.info("Event : " + json + " published successfully to kafka!!");
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

        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
    }


    public Map<RfqMetadataFieldNames, Object> fetchMeta(Rfq rfq) {
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        Map<RfqMetadataFieldNames, Object> extractorMetadata;

        //this.trades = new TradeDataLoader().loadTrades(this.session, "src/test/resources/trades/trades.json");
//        extractorMetadata = extractors.get(1).extractMetaData(rfq, session, trades);

//        metadata.put(RfqMetadataFieldNames.volumeTradedWeekToDate, 135L);
//        metadata.put(RfqMetadataFieldNames.volumeTradedMonthToDate, 12L);
//        metadata.put(RfqMetadataFieldNames.volumeTradedYearToDate, 12323L);

//        return metadata;

//        return extractorMetadata;

        //TODO: get metadata from each of the extractors
        for (RfqMetadataExtractor extractor : extractors) {
            extractorMetadata = extractor.extractMetaData(rfq, session, trades);
            extractorMetadata.forEach(metadata::putIfAbsent);
        }
//
        return metadata;
    }


}
