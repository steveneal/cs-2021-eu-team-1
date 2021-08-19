package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        //StructType schema = null;
        StructType schema =
                new StructType(new StructField[] {
                        new StructField("traderId", LongType, false, Metadata.empty()),
                        new StructField("entityId", LongType, false, Metadata.empty()),
                        new StructField("securityId", StringType, false, Metadata.empty()),
                        new StructField("lastQty", LongType, false, Metadata.empty()),
                        new StructField("lastPx", DoubleType, false, Metadata.empty()),
                        new StructField("tradeDate", DateType, false, Metadata.empty()),
                        new StructField("currency", StringType, false, Metadata.empty())
                });

        //TODO: load the trades dataset
        //Dataset<Row> trades = null;
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        log.info("Data fetched: " + trades.count());

        return trades;
    }

}
