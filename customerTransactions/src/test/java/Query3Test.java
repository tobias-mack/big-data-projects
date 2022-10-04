import org.junit.Test;

public class Query3Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "hdfs://localhost:9000/project1/customersTest.csv";
        input[1] = "hdfs://localhost:9000/project1/transactionsTest.csv";
        input[2] = "hdfs://localhost:9000/project1/output-query3.txt";

        Query2 wc = new Query2();
        wc.debug(input);
    }
}