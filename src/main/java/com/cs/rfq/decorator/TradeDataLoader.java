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
        //StructType schema = null;
//        StructType schema =
//                new StructType(new StructField[] {
//                        new StructField("TraderId", LongType, false, Metadata.empty()),
//                        new StructField("EntityId", LongType, false, Metadata.empty()),
//                        new StructField("MsgType", StringType, false, Metadata.empty()),
//                        new StructField("TraderReportId", LongType, false, Metadata.empty()),
//                        new StructField("PreviouslyReported", StringType, false, Metadata.empty()),
//                        new StructField("SecurityID", StringType, false, Metadata.empty()),
//                        new StructField("SecurityIdSource", LongType, false, Metadata.empty()),
//                        new StructField("LastQty", LongType, false, Metadata.empty()),
//                        new StructField("LastPx", DoubleType, false, Metadata.empty()),
//                        new StructField("TradeDate", DateType, false, Metadata.empty()),
//                        new StructField("TransactTime", DateType, false, Metadata.empty()),
//                        new StructField("NoSides", LongType, false, Metadata.empty()),
//                        new StructField("Side", LongType, false, Metadata.empty()),
//                        new StructField("OrderID", LongType, false, Metadata.empty()),
//                        new StructField("currency", StringType, false, Metadata.empty())
//                });

        StructType schema = new StructType(new StructField[] {
                new StructField("TraderId", LongType, false,  Metadata.empty()),
                new StructField("EntityId", LongType, false,  Metadata.empty()),
                new StructField("SecurityID", StringType, false, Metadata.empty()),
                new StructField("LastQty", LongType, false, Metadata.empty()),
                new StructField("LastPx", DoubleType, false, Metadata.empty()),
                new StructField("TradeDate", DateType, false, Metadata.empty()),
                new StructField("Currency", StringType, false, Metadata.empty())
        });

//        schema = createSchema(session, path);

        // 20180609-23:26:28



        //TimestampType
        // {'TraderId':1509345351319978288,
        // 'EntityId':5561279226039690843,
        // 'MsgType':35,
        // 'TradeReportId':6524728403119584392,
        // 'PreviouslyReported':'N',
        // 'SecurityID':'AT0000A105W3',
        // 'SecurityIdSource':4,
        // 'LastQty':400000,
        // 'LastPx':111.888,
        // 'TradeDate':'2018-06-10',
        // 'TransactTime':'20180610-05:52:27',
        // 'NoSides':1, 'Side':2,
        // 'OrderID':2224537576111440679,
        // 'Currency':'EUR'}

        // trades.json and loader-test-trades.json are identical
        // volume-traded-1 is different - it has no MsgType



        //TODO: load the trades dataset
        //Dataset<Row> trades = null;

        //session.read().options()
        Dataset<Row> trades = session.read().schema(schema).json(path);



        System.out.println();
        //trades.printSchema();
        session.read().json(path).printSchema();
        System.out.println();

        //TODO: log a message indicating number of records loaded and the schema used
        log.info("Data fetched: " + trades.count());

        return trades;
    }

    public StructType createSchema(SparkSession session, String path) {

        String json;

        try {
            BufferedReader buffReader = new BufferedReader(new FileReader(path));
            json = buffReader.readLine();
            System.out.println("Printing json");
            System.out.println(json);
            System.out.println();
            System.out.println();
            JsonObject jsonObject = (new JsonParser()).parse(json).getAsJsonObject();

            //jsonObject.


            buffReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }



        return null;
    }

}
