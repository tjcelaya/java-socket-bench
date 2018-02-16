package co.tjcelaya;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.err;
import static java.lang.System.out;

/**
 * Hello world!
 */
public class BenchCPU implements Runnable {

    @CommandLine.Option(names = {"-v", "--verbose"})
    private static boolean verbose = false;

    @CommandLine.Option(names = {"-t", "--threads"},
                        description = "Number of worker threads. The listener hands sockets off to them for responses.")
    protected static int threadCount = 1;

    private static final Timer TIMER;
    private static final Counter TOTAL;

    static {
        final MetricRegistry registry = new MetricRegistry();
        TOTAL = registry.counter(MetricRegistry.name(BenchCPU.class, "total"));
        TIMER = registry.timer(MetricRegistry.name(BenchCPU.class, "timer"));
    }

    public static void main(final String[] args) {
        CommandLine.run(new BenchCPU(), out, args);
    }

    public void run() {
        // final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(threadCount);
        final SynchronousQueue<Runnable> queue = new SynchronousQueue<>();
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, queue, rjeHandler);

        final ScheduledExecutorService statsPool = new ScheduledThreadPoolExecutor(1);
        statsPool.scheduleAtFixedRate(new StatsReporter(), 1, 1, TimeUnit.SECONDS);

        while (!pool.isTerminating()) {
            pool.submit(new BusyWorker());
        }
    }

    private static final class BusyWorker implements Runnable {

        private int counter = 0;

        public void run() {
            final Timer.Context timeCtx = TIMER.time();
            while (counter < Integer.MAX_VALUE) {
                counter++;
            }
            timeCtx.close();
            TOTAL.inc();

            if (verbose) {
                out.println(String.format("completed: %s, %d", Thread.currentThread().getName(), counter));
            }
        }
    }

    private static final class StatsReporter implements Runnable {

        @Override
        public void run() {
            try {
                out.println(
                        String.format(
                                "min (ns) %d max (ns) %d rate (Hz) mean %.02f " +
                                        "1m %.02f 5m %.02f 15m %.02f " +
                                        "total %d threads %d",
                                TIMER.getSnapshot().getMin(),
                                TIMER.getSnapshot().getMax(),
                                TIMER.getMeanRate(),
                                TIMER.getOneMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TIMER.getFiveMinuteRate(),
                                TOTAL.getCount(),
                                threadCount
                        )
                );
            } catch (final Exception e) {
                e.printStackTrace(err);
                // bail if we can't print stats
                System.exit(1);
            }
        }
    }

    private static final RejectedExecutionHandler rjeHandler = (r, tpe) -> {
        // Try indefinitely to add the task to the queue
        while (true)
        {
            if (tpe.isShutdown())  // If the executor is shutdown, reject the task and
                                   // throw RejectedExecutionException
            {
                throw new RejectedExecutionException("ThreadPool has been shutdown");
            }

            try
            {
                if (tpe.getQueue().offer(r, 1, TimeUnit.SECONDS))
                {
                    // Task got accepted!
                    break;
                }
            }
            catch (final InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    };

}
