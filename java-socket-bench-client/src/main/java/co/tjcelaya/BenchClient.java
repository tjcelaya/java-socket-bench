package co.tjcelaya;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
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
    private static final Timer TIMER;
    private static final Counter TOTAL;
    private static final Counter ERRORS;
    private static final Histogram TIME_DELTA_NS;

    static {
        final MetricRegistry registry = new MetricRegistry();
        TIMER = registry.timer(MetricRegistry.name(BenchClient.class, "connections"));
        TOTAL = registry.counter(MetricRegistry.name(BenchClient.class, "total"));
        ERRORS = registry.counter(MetricRegistry.name(BenchClient.class, "errors"));
        TIME_DELTA_NS = registry.histogram(MetricRegistry.name(BenchClient.class, "time_delta_ns"));
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

        final ScheduledExecutorService pool = new ScheduledThreadPoolExecutor(1);
        pool.scheduleAtFixedRate(() -> {
            try {
                out.println(
                        String.format(
                                "%s rate (rps) min %d mean %.05f max %d " +
                                        "1m %.05f 5m %.05f 15m %.05f " +
                                        "total %d errors %d " +
                                        "diff (min %d mean %.05f max %d)",
                                role,
                                TIMER.getSnapshot().getMin(),
                                TIMER.getMeanRate(),
                                TIMER.getSnapshot().getMax(),
                                TIMER.getOneMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TOTAL.getCount(),
                                ERRORS.getCount(),
                                TIME_DELTA_NS.getSnapshot().getMin(),
                                TIME_DELTA_NS.getSnapshot().getMean(),
                                TIME_DELTA_NS.getSnapshot().getMax()
                        )
                );
            } catch (final Exception e) {
                e.printStackTrace(err);
                System.exit(1);
            }
        }, 1, 1, TimeUnit.SECONDS);

        try {
            while (true) {

                final Timer.Context time = TIMER.time();

                try (final Socket s = new Socket(serverIp, serverPort);
                     final BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()))) {

                    if (zeroLinger) {
                        if (verbose) {
                            out.println("setting socket linger to true + zero");
                        }

                        s.setKeepAlive(false);
                        s.setSoLinger(true, 0);
                    }

                    final String server_ns = input.readLine();
                    final long delta_ns = System.nanoTime() - Long.parseUnsignedLong(server_ns);

                    if (verbose) out.println("delta nanos: " + delta_ns);

                    time.close();
                    TIME_DELTA_NS.update(delta_ns);
                } catch (final Exception e) {
                    ERRORS.inc();
                } finally {
                    TOTAL.inc();
                }
            }
        } catch (final Exception e) {
            e.printStackTrace(err);
        }
    }
}
