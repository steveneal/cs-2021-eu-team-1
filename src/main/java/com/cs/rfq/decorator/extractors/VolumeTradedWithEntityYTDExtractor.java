package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithEntityYTDExtractor implements RfqMetadataExtractor {

    private String since;
    private ArrayList<String> sinceDates = new ArrayList<String>();

    public VolumeTradedWithEntityYTDExtractor() {
        this.since = DateTime.now().toString("yyyy-MM-dd");
        DateTime sinceDate = DateTime.parse(since);
        this.sinceDates.add(sinceDate.minusWeeks(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(sinceDate.minusMonths(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(sinceDate.minusYears(1).toString("yyyy-MM-dd"));
    }

//    private Object getResults(Rfq rfq, SparkSession session, Dataset<Row> trades, String since) {
//
//    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        ArrayList<Long> volumeList = new ArrayList<>();
        for (String sinceVar : sinceDates){
            System.out.println(sinceVar);
            //long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
            //new DateTime.

//        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
//        long sinceMilliSeconds =  0L;
//        try {
//            java.util.Date d = f.parse(since);
//            sinceMilliSeconds = d.getTime();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }

//        long pastWeekMs = DateTime.now().withMillis(sinceMilliSeconds).minusWeeks(1).getMillis();
//        long pastMonths = DateTime.now().withMillis(sinceMilliSeconds).minusMonths(1).getMillis();
            String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                    rfq.getEntityId(),
                    rfq.getIsin(),
                    sinceVar);

            System.out.println("============= ERROR ============");
            System.out.println(trades);
            System.out.println();
            trades.createOrReplaceTempView("trade");
            Dataset<Row> sqlQueryResults = session.sql(query);


            Object volume = sqlQueryResults.first().get(0);
            if (volume == null) {
                volume = 0L;
            }

//        String[] sinceDates = {
//                since
//                new Date(pastMonths).toString(),
//                new Date(pastWeekMs).toString()
//        };
            volumeList.add((Long)volume);

            //results.put(RfqMetadataFieldNames.volumeTradedYearToDate, getResults(rfq, session, trades, sinceDates[0]));
//        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, getResults(rfq, session, trades, sinceDates[1]));
//        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, getResults(rfq, session, trades, sinceDates[2]));
        }

        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, volumeList.get(0));
        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, volumeList.get(1));
        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, volumeList.get(2));

        return results;
    }

    protected void setSince(String since) {
        this.since = since;
        DateTime sinceDate = DateTime.parse(since);
        this.sinceDates.clear();
        this.sinceDates.add(sinceDate.minusWeeks(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(sinceDate.minusMonths(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(sinceDate.minusYears(1).toString("yyyy-MM-dd"));
    }
}

