package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.averageTradedPricePastWeek;
import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.rfqId;

public class RfqIdExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(rfqId, rfq.getId());
        return results;
    }
}
