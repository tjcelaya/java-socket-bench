package co.tjcelaya;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.err;
import static java.lang.System.out;

public class BenchServer implements Runnable {

    private static final String role = "server";

    @CommandLine.Option(names = {"-p", "--port"},
                        description = "Port to bind to.",
                        required = true)
    private static int port = 0;

    @CommandLine.Option(names = {"-v", "--verbose"})
    private static boolean verbose = false;

    private static final Timer TIMER;
    private static final Counter TOTAL;
    private static final Counter ERRORS;

    static {
        final MetricRegistry registry = new MetricRegistry();
        TIMER = registry.timer(MetricRegistry.name(BenchServer.class, "connections"));
        TOTAL = registry.counter(MetricRegistry.name(BenchServer.class, "total"));
        ERRORS = registry.counter(MetricRegistry.name(BenchServer.class, "errors"));
    }

    public static void main(final String[] args) {
        CommandLine.run(new BenchServer(), out, args);
    }

    @Override
    public void run() {
        if (port == 0) {
            throw new RuntimeException("Port is required");
        }

        final ScheduledExecutorService pool = new ScheduledThreadPoolExecutor(1);
        pool.scheduleAtFixedRate(() -> {
            try {
                out.println(
                        String.format(
                                "%s rate (rps) min %d mean %.05f max %d " +
                                        "1m %.05f 5m %.05f 15m %.05f " +
                                        "total %d errors %d ",
                                role,
                                TIMER.getSnapshot().getMin(),
                                TIMER.getMeanRate(),
                                TIMER.getSnapshot().getMax(),
                                TIMER.getOneMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TOTAL.getCount(),
                                ERRORS.getCount()
                        )
                );
            } catch (final Exception e) {
                e.printStackTrace(err);
                System.exit(1);
            }
        }, 1, 1, TimeUnit.SECONDS);


        try (final ServerSocket listener = new ServerSocket(port)) {
            out.println("listening: " + port);

            while (true) {
                if (verbose) out.println("accepting");
                try (final Socket socket = listener.accept()) {

                    final Timer.Context time = TIMER.time();

                    try (final PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
                        final long ns = System.nanoTime();
                        if (verbose) out.println("server nanos: " + ns);
                        writer.println(ns);
                    } catch (final Exception e) {
                        ERRORS.inc();
                    } finally {
                        TOTAL.inc();
                    }

                    time.close();
                }
            }
        } catch (final Exception e) {
            e.printStackTrace(err);
        }
    }
}
