package com.cs.rfq.decorator;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);


    /**
     *
     *
     * TIMESTAMP: YYYY-MM-DD hh:mm:ss
     * DATETYPE: YYYY-MM-DD
     *
     *
     * @param session
     * @param path
     * @return
     */
    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files

        StructType schema = new StructType(new StructField[] {
                new StructField("TraderId", LongType, false,  Metadata.empty()),
                new StructField("EntityId", LongType, false,  Metadata.empty()),
                new StructField("SecurityID", StringType, false, Metadata.empty()),
                new StructField("LastQty", LongType, false, Metadata.empty()),
                new StructField("LastPx", DoubleType, false, Metadata.empty()),
                new StructField("TradeDate", DateType, false, Metadata.empty()),
                new StructField("Currency", StringType, false, Metadata.empty()),
                new StructField("Side", IntegerType, false, Metadata.empty()),
        });

        //TODO: load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        log.info("Data fetched: " + trades.count());

        return trades;
    }
}
