import junit.framework.TestCase;
import org.junit.Test;

public class OutlierDetectionTest extends TestCase {

    @Test
    public void testDebug() throws Exception {

        String[] input = new String[4];
        input[0] = "hdfs://localhost:9000/cs585/DataSetP.csv";
        input[1] = "hdfs://localhost:9000/cs585/output";
        input[2] = "50"; //k
        input[3] = "6000"; //r

        OutlierDetection o = new OutlierDetection();
        o.debug(input);


    }

}