package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;



public class AverageTradedPriceExtractor implements RfqMetadataExtractor {
    private String since;

//    public AverageTradedPriceExtractor() {
//        this.since = DateTime.now().minusWeeks(1).toString("yyyy-MM-dd");
//    }
    public AverageTradedPriceExtractor(){
        this.since = DateTime.now().toString("yyyy-MM-dd");
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.parse(since).withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.parse(since).withMillis(todayMs).minusWeeks(1).getMillis();

//        String query = String.format("SELECT avg(LastPx) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
//                rfq.getEntityId(),
//                rfq.getIsin(),
//                since);

        String query = String.format("SELECT avg(LastPx) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                new java.sql.Date(pastWeekMs));
        System.out.println("==============");
        System.out.println("TRADES INSIDE Avg EXTRACTOR");
        System.out.println(trades);
        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object averageTradePrice = sqlQueryResults.first().get(0);
        if (averageTradePrice == null) {
            averageTradePrice = 0.0;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradedPricePastWeek, averageTradePrice);
        return results;
    }

    public void setSince(String since) {
        if (!since.matches("\\d{4}-\\d{2}-\\d{2}")){
            throw new ValueException("Incorrect date format");
        }
        this.since = since;
    }
}
