package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;



public class AverageTradedPriceExtractor implements RfqMetadataExtractor {
    private String since;

    public AverageTradedPriceExtractor() {
        this.since = DateTime.now().minusWeeks(1).toString("yyyy-MM-dd");
    }
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT avg(LastPx) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object averageTradePrice = sqlQueryResults.first().get(0);
        if (averageTradePrice == null) {
            averageTradePrice = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradedPricePastWeek, averageTradePrice);
        return results;
    }
}
