import org.junit.Test;

import static org.junit.Assert.*;

public class Query2Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "hdfs://localhost:9000/cs585/customers.csv";
        input[1] = "hdfs://localhost:9000/cs585/transactions.csv";
        input[2] = "hdfs://localhost:9000/cs585/output2.txt";

        Query2 wc = new Query2();
        wc.debug(input);
    }
}
