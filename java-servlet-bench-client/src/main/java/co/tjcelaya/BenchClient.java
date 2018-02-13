package co.tjcelaya;


import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;

public class BenchClient implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BenchClient.class);

    @CommandLine.Option(names = {"-v", "--verbose"},
                        description = "Verbose")
    private boolean verbose = false;

    @CommandLine.Parameters(index = "0",
                            arity = "1")
    private String serverUri = "";

    @CommandLine.Option(names = {"-t", "--time"},
                        description = "Time in seconds for test run.")
    private long testSeconds = 10;

    private static final StopWatch STOP_WATCH = new StopWatch();

    public static void main(final String[] args) {
        CommandLine.run(new BenchClient(), System.out, args);
    }

    @Override
    public void run() {

        final Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.

        final URI uri;

        if (serverUri.isEmpty()) {
            throw new RuntimeException("Server address must be provided.");
        }

        try {
            uri = new URI(serverUri);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        final SocketConfig ngrinderSocketConfig = SocketConfig.custom().setSoLinger(0).setSoKeepAlive(false).build();
        final CloseableHttpClient client = HttpClientBuilder.create()
                .setDefaultSocketConfig(ngrinderSocketConfig)
                .build();

        final HttpUriRequest req = new HttpPost(uri);

        while (true) {
            try {
                try (final CloseableHttpResponse response = client.execute(req)) {
                    LOG.info("request completed, status: " + response.getStatusLine().getStatusCode());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
