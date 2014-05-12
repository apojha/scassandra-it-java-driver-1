import com.datastax.driver.core.Cluster;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MetaDataPriming {
    public static final int binaryPort = 4565;
    public static final int adminPort = 2343;
    public static final String CUSTOM_CLUSTER_NAME = "custom cluster name";
    private static Cluster cluster;
    public static Scassandra scassandraServer = ScassandraFactory.createServer(binaryPort, adminPort);
    private static PrimingClient primingClient = PrimingClient.builder().withPort(adminPort).build();
    private static ActivityClient activityClient = ActivityClient.builder().withAdminPort(adminPort).build();

    @BeforeClass
    public static void setup() throws Exception {
        scassandraServer.start();
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
    public void testPrimingOfClusterName() {
        //then
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of("tokens",ColumnTypes.Set);
        String query = "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'";
        Map<String, Object> row = new HashMap<>();
        row.put("cluster_name", CUSTOM_CLUSTER_NAME);
        row.put("partitioner","org.apache.cassandra.dht.Murmur3Partitioner");
        row.put("data_center","dc1");
        row.put("tokens", Sets.newHashSet("1743244960790844724"));
        row.put("rack","rc1");
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build();
        primingClient.primeQuery(prime);

        //when
        cluster = Cluster.builder().addContactPoint("localhost")
                .withPort(binaryPort).build();
        cluster.connect();

        //then
        assertEquals(CUSTOM_CLUSTER_NAME, cluster.getMetadata().getClusterName());
    }
}
