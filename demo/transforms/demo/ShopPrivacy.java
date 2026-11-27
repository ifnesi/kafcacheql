package demo;

import io.github.ifnesi.kafcacheql.transform.RecordTransform;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The demo's record transform for the shop-customer-notes topic, selected in kafcacheql.properties with
 * {@code topic.shop-customer-notes.transform=demo.ShopPrivacy}.
 *
 * <p>The driver calls {@link #apply} once for each record it fetches from Kafka, after decoding and before
 * the record is written to the local cache. Whatever this method returns is what gets stored, so anything it
 * removes never reaches the cache file or query results. Kafka itself still holds the original record.
 *
 * <p>For each note it:
 * <ol>
 *   <li>replaces the empty {@code age_band} field with a band derived from {@code birth_date}, so queries can
 *       group customers by age without seeing their birth dates;</li>
 *   <li>replaces any email address in the free-text {@code text} field with "[email removed]".</li>
 * </ol>
 * The settings in kafcacheql.properties then run on the result: they drop {@code birth_date} and the key, and
 * hash the {@code email} field. The class can still read {@code birth_date} here because those settings
 * run after it.
 *
 * <p>One instance is shared by several decoding threads at once, so the class keeps no mutable state. It
 * uses only the JDK and the driver's {@link RecordTransform} interface.
 */
public final class ShopPrivacy implements RecordTransform {
    // Compiled once and shared: a Pattern is safe to use from several threads.
    private static final Pattern EMAIL = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");

    @Override @SuppressWarnings("unchecked") public Record apply(Record record) {
        // The value arrives as plain Java objects: a JSON object is a mutable Map<String, Object>. Anything
        // else (such as a null value) is returned unchanged.
        if (record.value() instanceof Map<?, ?>) {
            var value = (Map<String, Object>) record.value();

            // Age at the time of the record, using Kafka's record timestamp rather than the current time, so
            // the result is the same every time the record is fetched or the cache is rebuilt.
            int age = Period.between(LocalDate.parse((String) value.get("birth_date")),
                    java.time.Instant.ofEpochMilli(record.timestamp()).atZone(ZoneOffset.UTC).toLocalDate()).getYears();
            value.put("age_band", age < 25 ? "under 25" : age < 45 ? "25–44" : "45+");

            // Free text can hold anything, so field settings can't clean it; redact email addresses here.
            value.put("text", EMAIL.matcher((String) value.get("text")).replaceAll("[email removed]"));
        }
        // The same record, changed in place. Returning null instead would leave the record out of the cache.
        return record;
    }
}
