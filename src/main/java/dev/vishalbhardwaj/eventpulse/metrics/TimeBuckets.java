package dev.vishalbhardwaj.eventpulse.metrics;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/** Minute-bucket helpers shared by the consumer and tests. */
public final class TimeBuckets {

    private TimeBuckets() {
    }

    /** Truncates epoch millis to the start of its minute (UTC). */
    public static LocalDateTime truncateToMinute(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MINUTES);
    }

    /** Redis key for a per-minute aggregate, e.g. {@code eventpulse:revenue:minute:20260920T1130}. */
    public static String minuteKey(String prefix, long epochMillis) {
        LocalDateTime m = truncateToMinute(epochMillis);
        return "%s:%04d%02d%02dT%02d%02d".formatted(prefix,
                m.getYear(), m.getMonthValue(), m.getDayOfMonth(), m.getHour(), m.getMinute());
    }
}
