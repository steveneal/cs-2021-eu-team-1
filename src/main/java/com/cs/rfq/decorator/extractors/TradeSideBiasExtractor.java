package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.averageTradedPricePastWeek;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {

    private ArrayList<String> sinceDates = new ArrayList<String>();

    public TradeSideBiasExtractor() {
        this.sinceDates.add(DateTime.now().minusWeeks(1).toString("yyyy-MM-dd"));
        this.sinceDates.add(DateTime.now().minusMonths(1).toString("yyyy-MM-dd"));
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        ArrayList<Double> tradeBiasList = new ArrayList<>();

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

            Object tradeBiasBuy = sqlQueryResultsBuy.first().get(0) == null ? -1.0 : sqlQueryResultsBuy.first().get(0);
            Object tradeBiasSell = sqlQueryResultsSell.first().get(0) == null ? -1.0 : sqlQueryResultsSell.first().get(0);

            tradeBiasList.add((double)tradeBiasBuy / (double)tradeBiasSell);
        }

        results.put(RfqMetadataFieldNames.tradeBiasPastWeek, tradeBiasList.get(0));
        results.put(RfqMetadataFieldNames.tradeBiasPastMonth, tradeBiasList.get(1));

        return results;
    }
}
