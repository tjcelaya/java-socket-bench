package co.tjcelaya;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    @CommandLine.Option(names = {"-t", "--threads"},
                        description = "Number of worker threads. The listener hands sockets off to them for responses.")
    private static int threadCount = 1;

    private final ConcurrentLinkedQueue<TimedSocket> clients = new ConcurrentLinkedQueue<>();

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

        final ScheduledExecutorService statsPool = new ScheduledThreadPoolExecutor(1);
        statsPool.scheduleAtFixedRate(() -> {
            try {
                out.println(
                        String.format(
                                "%s min (ns) %d max (ns) %d rate (Hz) mean %.02f " +
                                        "1m %.02f 5m %.02f 15m %.02f " +
                                        "total %d errors %d threads %d backlog %d",
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
                                clients.size()
                        )
                );
            } catch (final Exception e) {
                e.printStackTrace(err);
                System.exit(1);
            }
        }, 1, 1, TimeUnit.SECONDS);

        final Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threads.length; i++) {
            final Thread t = new Thread(new ServerWorker(clients));
            t.start();
            threads[i] = t;
        }

        out.println(String.format("server threads started (%d)", threadCount));

        try (final ServerSocket listener = new ServerSocket(port)) {
            out.println("listening: " + port);

            while (true) {
                clients.offer(new TimedSocket(listener.accept(), TIMER.time()));
            }
        } catch (final Exception e) {
            e.printStackTrace(err);
        }
    }

    private static final class TimedSocket implements Closeable {
        protected final Socket socket;
        protected final Timer.Context timerCtx;

        TimedSocket(final Socket socket, final Timer.Context timerCtx) {
            this.socket = socket;
            this.timerCtx = timerCtx;
        }

        @Override
        public void close() throws IOException {
            socket.close();
            timerCtx.close();
        }
    }

    private static final class ServerWorker implements Runnable {

        private final ConcurrentLinkedQueue<TimedSocket> clients;

        ServerWorker(final ConcurrentLinkedQueue<TimedSocket> clients) {
            this.clients = clients;
        }

        @Override
        public void run() {
            if (verbose) out.println("accepting");
            while (true) {
                final TimedSocket client = clients.poll();
                if (client == null) {
                    continue;
                }

                try (final PrintWriter writer = new PrintWriter(client.socket.getOutputStream(), true)) {
                    final long ns = System.nanoTime();
                    if (verbose) out.println("server nanos: " + ns);
                    writer.println(ns);
                    client.socket.close();
                    client.timerCtx.close();
                } catch (final Exception e) {
                    ERRORS.inc();
                } finally {
                    TOTAL.inc();
                }
            }
        }
    }
}
