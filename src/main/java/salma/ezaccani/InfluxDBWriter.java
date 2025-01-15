package salma.ezaccani;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.time.Instant;

public class InfluxDBWriter {
    private static final String token = "CpqxugFKzn1Eslsor72Ukzh2E1mn_J0LNG11wjJQQg7g6p-Fgk1JjrYHZmSEZBDmmYOGpSq5qt7MxJoyePpBlw==";
    private static final String bucket = "fraud-detection-bucket";
    private static final String org = "fraud-detection-org";
    private static final InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());

    public static void writeTransaction(String userId, double amount, String timestamp) {
        Point point = Point.measurement("suspicious_transactions")
                .addTag("userId", userId)
                .addField("amount", amount)
                .time(Instant.parse(timestamp), WritePrecision.MS);
        client.getWriteApiBlocking().writePoint(bucket, org, point);
    }
}
