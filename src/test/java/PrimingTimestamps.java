import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PrimingTimestamps {

    public static final int binaryPort = 4567;
    public static final int adminPort = 2345;
    private static Cluster cluster;
    public static Scassandra scassandraServer = ScassandraFactory.createServer(binaryPort, adminPort);
    public static PrimingClient primingClient = PrimingClient.builder().withPort(adminPort).build();
    public static final ActivityClient activityClient = ActivityClient.builder().withAdminPort(adminPort).build();

    @BeforeClass
    public static void setup() throws Exception {
        scassandraServer.start();
        cluster = Cluster.builder().addContactPoint("localhost")
                .withPort(binaryPort).build();
    }

    @AfterClass
    public static void shutdown() {
        cluster.shutdown();
        scassandraServer.stop();
    }

    @Before
    public void clearPrimes() {
        primingClient.clearPrimes();
        activityClient.clearQueries();
        activityClient.clearConnections();
    }

    @Test
    public void testPrimeWithRows() {
        String query = "select * from people";
        long now = System.currentTimeMillis();
        Date nowAsDate = new Date(now);
        System.out.println(now + " ::: " + nowAsDate);
        Map<String, Object> row = ImmutableMap.of("atimestamp", (Object) new Long(nowAsDate.getTime()));
        Map<String, ColumnTypes> types = ImmutableMap.of("atimestamp", ColumnTypes.Timestamp);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withRows(row)
                .withColumnTypes(types)
                .build();
        primingClient.primeQuery(prime);

        Session keyspace = cluster.connect("keyspace");
        ResultSet result = keyspace.execute(query);

        List<Row> allRows = result.all();
        assertEquals(1, allRows.size());
        assertEquals(nowAsDate, allRows.get(0).getDate("atimestamp"));
    }
}
