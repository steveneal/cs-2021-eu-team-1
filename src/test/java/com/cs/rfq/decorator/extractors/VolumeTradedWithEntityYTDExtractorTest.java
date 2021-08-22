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

public class VolumeTradedWithEntityYTDExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");


        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() {

        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        extractor.setSince("2019-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedYearToDate);

        assertEquals(1_350_000L, result);
    }


    @Test
    public void checkVolumeLastMonth() {
        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();

        extractor.setSince("2021-08-11");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedMonthToDate);
        assertEquals(850_000L, result);
    }


    @Test
    public void checkVolumeLastWeek() {
        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        extractor.setSince("2021-08-09");
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);
        //extractor.setSince();
        Object result = meta.get(RfqMetadataFieldNames.volumeTradedWeekToDate);
        assertEquals(850_000L, result);
    }


    @Test
    public void checkVolumeWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        extractor.setSince("2021-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedYearToDate);

        assertEquals(0L, result);
    }

    @Test
    public void testSetSinceException() {
        VolumeTradedWithEntityYTDExtractor extractor = new VolumeTradedWithEntityYTDExtractor();
        assertThrows(DateTimeException.class, () -> extractor.setSince("2-1-1"));
    }

}

