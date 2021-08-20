package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
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
        this.sinceDates.add(DateTime.now().minusWeeks(1).toString());
        this.sinceDates.add(DateTime.now().minusMonths(1).toString());
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        ArrayList<Double> tradeBiasList = new ArrayList<>();

        for (String sinceVar : sinceDates){
            String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side=1 AND TradeDate >= '%s' UNION ALL SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND Side=2 AND TradeDate >= '%s'",
                    rfq.getEntityId(),
                    rfq.getIsin(),
                    sinceVar);

            trades.createOrReplaceTempView("trade");
            Dataset<Row> sqlQueryResults = session.sql(query);

            Object tradeBiasBuy = sqlQueryResults.first().get(0) == null ? -1.0 : sqlQueryResults.first().get(0);
            Object tradeBiasSell = sqlQueryResults.first().get(1) == null ? -1.0 : sqlQueryResults.first().get(1);

            tradeBiasList.add((double)tradeBiasBuy / (double)tradeBiasSell);
        }

        results.put(RfqMetadataFieldNames.tradeBiasPastWeek, tradeBiasList.get(0));
        results.put(RfqMetadataFieldNames.tradeBiasPastMonth, tradeBiasList.get(1));

        return results;
    }
}
