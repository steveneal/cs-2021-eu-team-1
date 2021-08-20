package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithEntityYTDExtractor implements RfqMetadataExtractor {

    private String since;

    public VolumeTradedWithEntityYTDExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }

    private Object getResults(Rfq rfq, SparkSession session, Dataset<Row> trades, String since) {
        String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        return volume;
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        String[] sinceDates = {
                since,
                DateTime.now().getYear() + "-" + DateTime.now().getMonthOfYear() + "-01",
                new java.sql.Date(pastWeekMs).toString()
        };

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, getResults(rfq, session, trades, sinceDates[0]));
        results.put(RfqMetadataFieldNames.volumeTradedMonthToDate, getResults(rfq, session, trades, sinceDates[1]));
        results.put(RfqMetadataFieldNames.volumeTradedWeekToDate, getResults(rfq, session, trades, sinceDates[2]));
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
