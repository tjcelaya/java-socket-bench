package co.tjcelaya;


import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.err;
import static java.lang.System.out;

public class BenchClient implements Runnable {

    private static final String role = "client";

    @CommandLine.Option(names = {"-v", "--verbose"})
    private static boolean verbose = false;

    @CommandLine.Option(names = {"-l", "--zero-linger"})
    private static boolean zeroLinger = false;

    @CommandLine.Parameters(index = "0",
                            arity = "1")
    private String serverIp = "";

    @CommandLine.Parameters(index = "1",
                            arity = "1")
    private int serverPort = 0;

    @CommandLine.Option(names = {"-t", "--threads"},
                        description = "Threads which can create and close connections.")
    private static int threadCount = 1;

    private static final Timer TIMER;
    private static final Counter TOTAL;
    private static final Counter ERRORS;

    static {
        final MetricRegistry registry = new MetricRegistry();
        TIMER = registry.timer(MetricRegistry.name(BenchClient.class, "connections"));
        TOTAL = registry.counter(MetricRegistry.name(BenchClient.class, "total"));
        ERRORS = registry.counter(MetricRegistry.name(BenchClient.class, "errors"));
    }


    public static void main(final String[] args) {
        CommandLine.run(new BenchClient(), out, args);
    }

    @Override
    public void run() {

        if (zeroLinger) {
            out.println("enabling linger with timeout 0");
        } else {
            out.println("not enabling linger");
        }

        final ScheduledExecutorService statsPool = new ScheduledThreadPoolExecutor(1);
        statsPool.scheduleAtFixedRate(() -> {
            try {
                out.println(
                        String.format(
                                "%s min (ns) %d max (ns) %d rate (Hz) mean %.02f " +
                                        "1m %.02f 5m %.02f 15m %.02f " +
                                        "total %d errors %d threads %d linger %b",
                                role,
                                TIMER.getSnapshot().getMin(),
                                TIMER.getSnapshot().getMax(),
                                TIMER.getMeanRate(),
                                TIMER.getOneMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TOTAL.getCount(),
                                ERRORS.getCount(),
                                threadCount,
                                zeroLinger
                        )
                );
            } catch (final Exception e) {
                e.printStackTrace(err);
                System.exit(1);
            }
        }, 1, 1, TimeUnit.SECONDS);

        final ExecutorService clientPool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            clientPool.submit(new ClientWorker(serverIp, serverPort));
        }

        out.println(String.format("client threads started (%d)", threadCount));

        Runtime.getRuntime().addShutdownHook(new Thread(clientPool::shutdownNow));

        while (!clientPool.isTerminated()) {
            try {
                clientPool.awaitTermination(1L, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                e.printStackTrace(err);
            }
        }
    }

    private static final class ClientWorker implements Runnable {

        private final String serverIp;
        private final int serverPort;

        ClientWorker(final String serverIp, final int serverPort) {
            this.serverIp = serverIp;
            this.serverPort = serverPort;
        }

        @Override
        public void run() {
            while (true) {
                try (final Socket s = new Socket(serverIp, serverPort);
                     final Timer.Context time = TIMER.time();
                     final PrintWriter writer = new PrintWriter(s.getOutputStream(), true)) {
                    final long ns = System.nanoTime();
                    if (verbose) out.println("server nanos: " + ns);
                    writer.println(ns);
                } catch (final Exception e) {
                    ERRORS.inc();
                } finally {
                    TOTAL.inc();
                }
            }
        }
    }
}
