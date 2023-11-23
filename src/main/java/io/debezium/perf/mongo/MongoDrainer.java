package io.debezium.perf.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.*;

public class MongoDrainer {


    private static final int MAX_NO_EVENTS = 10;
    private final ConnectionString connectionString;
    private final String collection;
    private final List<Bson> pipeline;
    private final long eventsExpected;

    private Instant startTimestamp;
    private Instant endTimestamp;

    private long counter;

    public MongoDrainer(String url, String collection, long eventsExpected) {
        this.connectionString = new ConnectionString(url);
        this.collection = collection;
        this.pipeline = List.of(createOperationFilter());
        this.eventsExpected = eventsExpected;
    }

    private static Bson createOperationFilter() {
        return Aggregates.match(
                Filters.in(
                        "operationType",
                        Stream.of(INSERT,UPDATE,REPLACE,DELETE).map(OperationType::getValue).toList()));
    }

    private BsonTimestamp getOplogTimestamp() {
        try (var client = MongoClients.create(connectionString)) {
            var oplog = client.getDatabase("local").getCollection("oplog.rs");
            var query = oplog.find(new BsonDocument("ns", new BsonString(collection)), BsonDocument.class)
                    .limit(1)
                    .sort(Sorts.ascending("ts"));

            try (var cursor = query.cursor()) {
                return cursor.next().getTimestamp("ts");
            }
        }
    }

    public void run() {
        try (var client = MongoClients.create(connectionString)) {
            var timestamp = getOplogTimestamp();

            System.out.println("Capture will start at operation time: " + timestamp);
            System.out.println("Aggregation pipeline: " + pipeline);

            var stream = client
                    .watch(pipeline, BsonDocument.class)
                    .startAtOperationTime(timestamp);

            captureChanges(stream);
        }
    }

    private void captureChanges(ChangeStreamIterable<BsonDocument> stream) {
        System.out.println("Capturing changes from '" + connectionString +"'");
        var noEventCounter = 0;

        try(var cursor = stream.cursor()) {
            startTimestamp = Instant.now();
            do {
                var event = cursor.tryNext();
                if (event != null) {
                    counter++;
                } else {
                    noEventCounter++;
                }
            } while ((eventsExpected - counter > 0) || noEventCounter < MAX_NO_EVENTS);

            endTimestamp = Instant.now();
        }
    }
    public void printResults() {
        var duration = Duration.between(startTimestamp, endTimestamp);
        System.out.println("---");
        System.out.println("Total capture time in seconds: "  + duration.toSeconds());
        System.out.println("Total capture time in millis: "  + duration.toMillis());
        System.out.println("Total events seen: " + counter);
    }

    public static void main(String[] args) {
        var cs = args[0];
        var collection = args[1];
        var eventsExpected = Long.parseLong(args[2]);
//        var cs = "mongodb://localhost:27017,localhost:27117,localhost:27217/?replicaSet=rs0";
//        var collection = "test.items";
//        var eventsExpected = 200_001;

        var drainer = new MongoDrainer(cs, collection, eventsExpected);
        drainer.run();
        drainer.printResults();
    }
}
