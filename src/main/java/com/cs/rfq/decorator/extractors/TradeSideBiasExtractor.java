package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.averageTradedPricePastWeek;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {

    private String since;
    private ArrayList<String> sinceDates = new ArrayList<String>();

    public TradeSideBiasExtractor() {
        this.since = DateTime.now().toString("yyyy-MM-dd");
    }

    public static String GetQueryString(Rfq rfq, int side, long sinceLong){
        return String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                side,
                new java.sql.Date(sinceLong));
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = DateTime.parse(since).withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.parse(since).withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.parse(since).withMillis(todayMs).minusMonths(1).getMillis();

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        trades.createOrReplaceTempView("trade");

        Dataset<Row> queryResultsBuyWeek = session.sql(GetQueryString(rfq, 1, pastWeekMs));
        Dataset<Row> queryResultsBuyMonth = session.sql(GetQueryString(rfq, 1, pastMonthMs));
        Dataset<Row> queryResultsSellWeek = session.sql(GetQueryString(rfq, 2, pastWeekMs));
        Dataset<Row> queryResultsSellMonth = session.sql(GetQueryString(rfq, 2, pastMonthMs));

        Object buyWeek = queryResultsBuyWeek.first().get(0) == null ? -1L : queryResultsBuyWeek.first().get(0);
        Object buyMonth = queryResultsBuyMonth.first().get(0) == null ? -1L : queryResultsBuyMonth.first().get(0);
        Object sellWeek = queryResultsSellWeek.first().get(0) == null ? -1L : queryResultsSellWeek.first().get(0);
        Object sellMonth = queryResultsSellMonth.first().get(0) == null ? -1L : queryResultsSellMonth.first().get(0);

        results.put(RfqMetadataFieldNames.buySellRatioPastWeek, buyWeek.toString() + "/" + sellWeek.toString());
        results.put(RfqMetadataFieldNames.buySellRatioPastMonth, buyMonth.toString() + "/" + sellMonth.toString());


//        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
//        ArrayList<String> tradeBiasList = new ArrayList<>();
//
//
//
//        for (String sinceVar : sinceDates){
//            String queryBuy = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side=1 AND TradeDate >= '%s'",
//                    rfq.getEntityId(),
//                    rfq.getIsin(),
//                    sinceVar);
//            String querySell = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side=2 AND TradeDate >= '%s'",
//                    rfq.getEntityId(),
//                    rfq.getIsin(),
//                    sinceVar);
//
//            trades.createOrReplaceTempView("trade");
//            Dataset<Row> sqlQueryResultsBuy = session.sql(queryBuy);
//            Dataset<Row> sqlQueryResultsSell = session.sql(querySell);
//
//            Object tradeBiasBuy = sqlQueryResultsBuy.first().get(0) == null ? -1L : sqlQueryResultsBuy.first().get(0);
//            Object tradeBiasSell = sqlQueryResultsSell.first().get(0) == null ? -1L : sqlQueryResultsSell.first().get(0);
//            //tradeBiasList.add((long)tradeBiasBuy / (long)tradeBiasSell);
//            tradeBiasList.add(tradeBiasBuy.toString() + "/" + tradeBiasSell.toString());
//        }
//
//        results.put(RfqMetadataFieldNames.buySellRatioPastWeek, tradeBiasList.get(0));
//        results.put(RfqMetadataFieldNames.buySellRatioPastMonth, tradeBiasList.get(1));

        return results;
    }

    public void setSince(String since){
        if (!since.matches("\\d{4}-\\d{2}-\\d{2}")) {
            throw new DateTimeException("Incorrect date format");
        }
        this.since = since;
    }
}
