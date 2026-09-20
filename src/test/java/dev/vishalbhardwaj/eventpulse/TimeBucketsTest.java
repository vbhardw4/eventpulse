package dev.vishalbhardwaj.eventpulse;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import dev.vishalbhardwaj.eventpulse.metrics.TimeBuckets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeBucketsTest {

    @Test
    void truncatesToMinuteBoundary() {
        // 2026-09-20T11:17:22.500Z
        long epochMillis = 1789903042500L;
        LocalDateTime minute = TimeBuckets.truncateToMinute(epochMillis);
        assertEquals(2026, minute.getYear());
        assertEquals(9, minute.getMonthValue());
        assertEquals(20, minute.getDayOfMonth());
        assertEquals(11, minute.getHour());
        assertEquals(17, minute.getMinute());
        assertEquals(0, minute.getSecond());
        assertEquals(0, minute.getNano());
    }

    @Test
    void minuteKeyFormat() {
        long epochMillis = 1789903042500L;
        assertEquals("eventpulse:revenue:minute:20260920T1117",
                TimeBuckets.minuteKey("eventpulse:revenue:minute", epochMillis));
    }
}
