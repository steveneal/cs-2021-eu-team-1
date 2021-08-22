package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest {


    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }


    @Test
    public void testCorrectVolumeExtractedGivenInstrument() {
        InstrumentLiquidityExtractor instLiquidExtractor = new InstrumentLiquidityExtractor();

        instLiquidExtractor.setSince("2021-08-01");
        Map<RfqMetadataFieldNames, Object> map = instLiquidExtractor.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.totalTradesLiquidity);
        //results.put(RfqMetadataFieldNames.totalTradesLiquidity, volume);

        assertEquals(850_000L, result);
    }

    @Test
    public void testSetSinceException() {
        InstrumentLiquidityExtractor instLiquidExtractor = new InstrumentLiquidityExtractor();
        assertThrows(DateTimeException.class, () -> instLiquidExtractor.setSince("2-1-1"));
    }

}
