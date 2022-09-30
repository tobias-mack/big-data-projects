import org.junit.Test;

import static org.junit.Assert.*;

public class WordCountTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];
        input[0] = "hdfs://localhost:9000/cs585/data.txt";
        input[1] = "hdfs://localhost:9000/cs585/output3.txt";

        WordCount wc = new WordCount();
        wc.debug(input);
    }
}