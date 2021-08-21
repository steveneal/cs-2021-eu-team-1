package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

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
        DateTime sinceDate = DateTime.parse(since);
        this.sinceDates.add(sinceDate.minusWeeks(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(sinceDate.minusMonths(1).toString("yyyy-MM-dd"));
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        ArrayList<String> tradeBiasList = new ArrayList<>();

        for (String sinceVar : sinceDates){
            String queryBuy = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side=1 AND TradeDate >= '%s'",
                    rfq.getEntityId(),
                    rfq.getIsin(),
                    sinceVar);
            String querySell = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side=2 AND TradeDate >= '%s'",
                    rfq.getEntityId(),
                    rfq.getIsin(),
                    sinceVar);

            trades.createOrReplaceTempView("trade");
            Dataset<Row> sqlQueryResultsBuy = session.sql(queryBuy);
            Dataset<Row> sqlQueryResultsSell = session.sql(querySell);

            Object tradeBiasBuy = sqlQueryResultsBuy.first().get(0) == null ? -1L : sqlQueryResultsBuy.first().get(0);
            Object tradeBiasSell = sqlQueryResultsSell.first().get(0) == null ? -1L : sqlQueryResultsSell.first().get(0);
            //tradeBiasList.add((long)tradeBiasBuy / (long)tradeBiasSell);
            tradeBiasList.add(tradeBiasBuy.toString() + "/" + tradeBiasSell.toString());
        }

        results.put(RfqMetadataFieldNames.buySellRatioPastWeek, tradeBiasList.get(0));
        results.put(RfqMetadataFieldNames.buySellRatioPastMonth, tradeBiasList.get(1));

        return results;
    }

    public void setSince(String since){
        this.since = since;
        DateTime sinceDate = DateTime.parse(since);
        this.sinceDates.clear();
        this.sinceDates.add(sinceDate.minusWeeks(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(sinceDate.minusMonths(1).toString("yyyy-MM-dd"));
    }
}
