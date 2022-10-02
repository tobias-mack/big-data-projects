import org.junit.Test;

import static org.junit.Assert.*;

public class Query1Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];
        input[0] = "hdfs://localhost:9000/cs585/customers.csv";
        input[1] = "hdfs://localhost:9000/cs585/output.txt";

        Query1 wc = new Query1();
        wc.debug(input);
    }
}
