package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import scala.Function1;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithEntityYTDExtractor implements RfqMetadataExtractor {

    private String since;

    public VolumeTradedWithEntityYTDExtractor() {
        this.since = DateTime.now().toString("yyyy-MM-dd");
    }

//    private Object getResults(Rfq rfq, SparkSession session, Dataset<Row> trades, String since) {
//
//    }

    public static String GetQueryString(Rfq rfq, long sinceLong) {
        return String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                new java.sql.Date(sinceLong));
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = DateTime.parse(since).withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.parse(since).withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.parse(since).withMillis(todayMs).minusMonths(1).getMillis();
        long pastYearMs = DateTime.parse(since).withMillis(todayMs).minusYears(1).getMillis();

        trades.createOrReplaceTempView("trade");
        Dataset<Row> queryResultsWeek = session.sql(GetQueryString(rfq, pastWeekMs));
        Dataset<Row> queryResultsMonth = session.sql(GetQueryString(rfq, pastMonthMs));
        Dataset<Row> queryResultsYear = session.sql(GetQueryString(rfq, pastYearMs));

        Object volumeWeek = queryResultsWeek.first().get(0) == null ? 0L : queryResultsWeek.first().get(0);
        Object volumeMonth = queryResultsMonth.first().get(0) == null ? 0L : queryResultsMonth.first().get(0);
        Object volumeYear = queryResultsYear.first().get(0) == null ? 0L : queryResultsYear.first().get(0);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();


        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, volumeWeek);
        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, volumeMonth);
        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, volumeYear);



//        ArrayList<Long> volumeList = new ArrayList<>();
//        for (String sinceVar : sinceDates){
//            //long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
//            //new DateTime.
//
////        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
////        long sinceMilliSeconds =  0L;
////        try {
////            java.util.Date d = f.parse(since);
////            sinceMilliSeconds = d.getTime();
////        } catch (ParseException e) {
////            e.printStackTrace();
////        }
//
////        long pastWeekMs = DateTime.now().withMillis(sinceMilliSeconds).minusWeeks(1).getMillis();
////        long pastMonths = DateTime.now().withMillis(sinceMilliSeconds).minusMonths(1).getMillis();
//            String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
//                    rfq.getEntityId(),
//                    rfq.getIsin(),
//                    sinceVar);
//
//            System.out.println("============= ERROR ============");
//            System.out.println(trades);
//            System.out.println();
//            trades.createOrReplaceTempView("trade");
//            Dataset<Row> sqlQueryResults = session.sql(query);
//
//
//            Object volume = sqlQueryResults.first().get(0);
//            if (volume == null) {
//                volume = 0L;
//            }
//
////        String[] sinceDates = {
////                since
////                new Date(pastMonths).toString(),
////                new Date(pastWeekMs).toString()
////        };
//            volumeList.add((Long)volume);
//
//            //results.put(RfqMetadataFieldNames.volumeTradedYearToDate, getResults(rfq, session, trades, sinceDates[0]));
////        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, getResults(rfq, session, trades, sinceDates[1]));
////        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, getResults(rfq, session, trades, sinceDates[2]));
//        }

//        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, volumeList.get(0));
//        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, volumeList.get(1));
//        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, volumeList.get(2));

        return results;
    }

    protected void setSince(String since) {
        if (!since.matches("\\d{4}-\\d{2}-\\d{2}")){
            throw new DateTimeException("Incorrect date format");
        }
        this.since = since;
    }
}

