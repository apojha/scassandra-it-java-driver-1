import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.Query;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicPrimingTest {

    public static final int binaryPort = 4567;
    public static final int adminPort = 2345;
    private Cluster cluster;
    public static Scassandra scassandraServer = ScassandraFactory.createServer(binaryPort, adminPort);
    public static PrimingClient primingClient = new PrimingClient("localhost", adminPort);
    public static final ActivityClient activityClient = new ActivityClient("localhost", adminPort);

    @BeforeClass
    public static void setup() throws Exception {
        scassandraServer.start();
    }


    @AfterClass
    public static void shutdown() {
        scassandraServer.stop();
    }

    @After
    public void closeCluster() {
        cluster.shutdown();
    }

    @Test
    public void testUseAndSimpleQueryWithNoPrime() {
        cluster = Cluster.builder().addContactPoint("localhost")
                    .withPort(binaryPort).build();
        Query expectedUseKeyspace = new Query("use anykeyspace", "ONE");
        Query expectedSelect = new Query("select * from people", "ONE");


        Session keyspace = cluster.connect("anykeyspace");
        ResultSet result = keyspace.execute("select * from people");

        assertEquals(0, result.all().size());
        List<org.scassandra.http.client.Query> recordedQueries = activityClient.retrieveQueries();
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedUseKeyspace));
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedSelect));

    }

}
