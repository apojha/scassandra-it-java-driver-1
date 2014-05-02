import com.datastax.driver.core.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

import static org.junit.Assert.assertEquals;

public class PreparedStatementTest {
    public static final int binaryPort = 4566;
    public static final int adminPort = 2344;
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
    public void testPreparePreparedStatement() {
        //given
        //when
        Session keyspace = cluster.connect("keyspace");
        PreparedStatement prepare = keyspace.prepare("Select * from people where name = ?");
        BoundStatement boundStatement = prepare.bind("Chris");
        ResultSet results = keyspace.execute(boundStatement);
        //then
        assertEquals(0, results.all().size());
    }

}
