import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PreparedStatementTest {
    public static final int binaryPort = 4566;
    public static final int adminPort = 2344;
    private static Cluster cluster;

    private static Scassandra scassandraServer = ScassandraFactory.createServer(binaryPort, adminPort);
    private static PrimingClient primingClient = PrimingClient.builder().withPort(adminPort).build();
    private static final ActivityClient activityClient = ActivityClient.builder().withAdminPort(adminPort).build();

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
    public void preparedStatementWithoutPriming() {
        //given
        Session keyspace = cluster.connect("keyspace");
        //when
        PreparedStatement prepare = keyspace.prepare("select * from people where name = ?");
        BoundStatement boundStatement = prepare.bind("Chris");
        ResultSet results = keyspace.execute(boundStatement);
        //then
        assertEquals(0, results.all().size());
    }

    @Test
    public void primeAndExecuteAPreparedStatement() {
        //given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        primingClient.primePreparedStatement(PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from people where name = ?")
                .withRows(row)
                .build());
        Session keyspace = cluster.connect("keyspace");

        //when
        PreparedStatement prepare = keyspace.prepare("select * from people where name = ?");
        BoundStatement boundStatement = prepare.bind("Chris");
        ResultSet results = keyspace.execute(boundStatement);

        //then
        List<Row> asList = results.all();
        assertEquals(1, asList.size());
        assertEquals("Chris", asList.get(0).getString("name"));
    }

}
