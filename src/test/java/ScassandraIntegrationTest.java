import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.scassandra.ServerStubRunner;

import static org.junit.Assert.assertEquals;

public class ScassandraIntegrationTest {

    public static final int binaryPort = 4567;
    public static final int adminPort = 2345;
    private static ScassandraRunner scassandraRunner;
    private Cluster cluster;

    @BeforeClass
    public static void setup() throws Exception {
        scassandraRunner = new ScassandraRunner(binaryPort, adminPort);
        scassandraRunner.start();
    }

    @AfterClass
    public static void shutdown() {
        scassandraRunner.stop();
    }

    @After
    public void closeCluster() {
        cluster.shutdown();
    }

    @Test
    public void testUseAndSimpleQuery() {
        cluster = Cluster.builder().addContactPoint("localhost")
                    .withPort(binaryPort).build();

        Session keyspace = cluster.connect("anykeyspace");

        ResultSet result = keyspace.execute("select * from people");

        assertEquals(0, result.all().size());
    }

    static class ScassandraRunner {
        private final ServerStubRunner serverStubRunner;

        public ScassandraRunner(int binaryPort, int adminPort) {
            serverStubRunner = new ServerStubRunner(binaryPort, adminPort);
        }

        public void start() {
            new Thread() {
                public void run() {
                    serverStubRunner.start();
                }
            }.start();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void stop() {
            serverStubRunner.shutdown();
        }

    }

}
