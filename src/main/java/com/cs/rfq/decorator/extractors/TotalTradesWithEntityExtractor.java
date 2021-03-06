package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.time.DateTimeException;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

/*
User Story : Total Volume Traded for Instrument (1)
 */
public class TotalTradesWithEntityExtractor implements RfqMetadataExtractor {
    private String since;
    public TotalTradesWithEntityExtractor(){
        this.since = DateTime.now().toString("yyyy-MM-dd");
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

//        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
//        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
//        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();
        long todayMs = DateTime.parse(since).withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.parse(since).withMillis(todayMs).minusWeeks(1).getMillis();
        long pastYearMs = DateTime.parse(since).withMillis(todayMs).minusYears(1).getMillis();


        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));


        long tradesToday = filtered.filter(trades.col("TradeDate").$eq$eq$eq(new java.sql.Date(todayMs))).count();
        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).count();
        long tradesPastYear = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastYearMs))).count();

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradesWithEntityToday, tradesToday);
        results.put(tradesWithEntityPastWeek, tradesPastWeek);
        results.put(tradesWithEntityPastYear, tradesPastYear);
        return results;
    }

    public void setSince(String since){
        if (!since.matches("\\d{4}-\\d{2}-\\d{2}")) {
            throw new DateTimeException("Incorrect date format");
        }
        this.since = since;
    }
}
