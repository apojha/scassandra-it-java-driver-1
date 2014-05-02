import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicPrimingTest {

    public static final int binaryPort = 4567;
    public static final int adminPort = 2345;
    private static Cluster cluster;
    public static Scassandra scassandraServer = ScassandraFactory.createServer(binaryPort, adminPort);
    public static PrimingClient primingClient = new PrimingClient("localhost", adminPort);
    public static final ActivityClient activityClient = new ActivityClient("localhost", adminPort);

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
    public void testUseAndSimpleQueryWithNoPrime() {
        Query expectedUseKeyspace = new Query("use anykeyspace", "ONE");
        Query expectedSelect = new Query("select * from people", "ONE");


        Session keyspace = cluster.connect("anykeyspace");
        ResultSet result = keyspace.execute("select * from people");

        assertEquals(0, result.all().size());
        List<Query> recordedQueries = activityClient.retrieveQueries();
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedUseKeyspace));
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedSelect));

    }

    @Test
    public void testPrimeWithRows() {
        String query = "select * from people";
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        PrimingRequest prime = PrimingRequest.builder()
                .withQuery(query)
                .withRows(row)
                .build();
        primingClient.prime(prime);

        Session keyspace = cluster.connect("keyspace");
        ResultSet result = keyspace.execute(query);

        List<Row> allRows = result.all();
        assertEquals(1, allRows.size());
        assertEquals("Chris", allRows.get(0).getString("name"));
    }

}
