package bbyk.loadtests;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Most of the implementation is internalized in one method to be easily readable.
 *
 * @author bbyk
 */
public class MemcachedClientsLoadTest {
    private static final int MILLIS_PER_SECOND = 1000;
    private InetSocketAddress[] addresses;

    @BeforeClass
    public void setup() throws IOException {
        // Tell spy to use the SunLogger
        final Properties systemProperties = System.getProperties();
        systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        System.setProperties(systemProperties);

        // turn off logging
        Logger.getRootLogger().setLevel(Level.OFF);

        addresses = new InetSocketAddress[]{
                new InetSocketAddress("localhost", 11211)
        };
    }

    @Test
    public void testReadWriteProfiles() throws Exception {

        // params of the test
        final int[] params = new int[]{
                /* doc size, nthreads, actors, requests per second, seconds */
                5 * 1024, 300, 10000, 2000, 120,
                30 * 1024, 5, 10000, 1000, 1,
                5 * 1024, 5, 10000, 1000, 120,
                5 * 1024, 50, 10000, 1000, 120,
                30 * 1024, 100, 10000, 1000, 120,
                5 * 1024, 100, 10000, 1000, 120
        };

        System.out.println("tps - transactions per second");
        System.out.println("tt - total number transactions processed");
        System.out.println("mrps - memcached requests per second");
        System.out.println("qs - request queue size");
        System.out.println("err - number of errors");
        System.out.println("avtt - avarage transaction time in milliseconds");
        System.out.println();

        final ClientSetup[] clientSetups = ClientSetup.values();
        for (final ClientSetup clientSetup : clientSetups) {
            for (int i = 0; i < params.length; ) {
                testReadWriteProfileSafe(clientSetup, params[i++], params[i++], params[i++], params[i++], params[i++]);
            }
        }
    }

    private void testReadWriteProfileSafe(@NotNull final ClientSetup setup, final int docSize, final int threadCount,
                                          final int numberOfActors, final int requestsPerSecond,
                                          final int seconds) throws Exception {
        try {
            testReadWriteProfile(setup, docSize, threadCount, numberOfActors, requestsPerSecond, seconds);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testReadWriteProfile(@NotNull final ClientSetup setup, final int docSize, final int threadCount,
                                      final int numberOfActors, final int requestsPerSecond,
                                      final int seconds) throws Exception {
        System.out.printf("Setup: %s, document size: %d, threadCount: %d, numberOfActors: %d, requestsPerSecond: %d, seconds: %d",
                setup,
                docSize,
                threadCount,
                numberOfActors,
                requestsPerSecond,
                seconds);
        System.out.println();

        // prepare the seedBuffer
        final Random rnd = new Random();
        final byte[] seedBuffer = new byte[docSize];
        rnd.nextBytes(seedBuffer);

        final int totalNumberOfRequests = requestsPerSecond * seconds;

        // prepare shared state
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        final CountDownLatch allDone = new CountDownLatch(totalNumberOfRequests);
        final String actorPrefix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
        final AtomicInteger errorCount = new AtomicInteger();
        final ConcurrentHashMap<Class, Exception> errorSet = new ConcurrentHashMap<Class, Exception>();
        final ClientFactory clientFactory = new ClientFactory(setup, addresses);
        final AtomicInteger transactionCount = new AtomicInteger();
        final AtomicInteger reqCount = new AtomicInteger();
        final AtomicLong avgRespTime = new AtomicLong();
        final AtomicInteger avgRespTimeBase = new AtomicInteger();
        final AtomicInteger queueSize = new AtomicInteger();

        // for everything except PER_THREAD_SPY_MEMCACHED it's the same instance
        final ArrayBlockingQueue<BasicMemcachedClient> actorClients = new ArrayBlockingQueue<BasicMemcachedClient>(numberOfActors);
        final boolean[] actorInitialized = new boolean[numberOfActors];
        final Object[] actorLock = new Object[numberOfActors];
        for (int i = 0; i < numberOfActors; i++) {
            actorClients.offer(clientFactory.getOrCreate());
            actorLock[i] = i;
        }

        // prepare the queue of requests
        final int[] requests = new int[totalNumberOfRequests];
        for (int i = 0; i < totalNumberOfRequests; i++) {
            requests[i] = i % numberOfActors;
        }

        // let's emulate IO thread the feeds our threadpool with requests.
        final Thread ioThread = new Thread(new Runnable() {
            public void run() {
                try {
                    final int tickIntervalMs = 200;
                    final int requestsPerTick = requestsPerSecond * tickIntervalMs / MILLIS_PER_SECOND;

                    int i = 0;
                    long start = System.currentTimeMillis();
                    long leftover = 0;

                    while (i < totalNumberOfRequests) {
                        Thread.sleep(10);
                        final long now = System.currentTimeMillis();
                        final long elapsed = now - start + leftover;
                        final int ticks = (int) (elapsed / tickIntervalMs);
                        final int requestsToPush = ticks * requestsPerTick;

                        leftover = elapsed % tickIntervalMs;
                        start = now;

                        if (requestsToPush == 0)
                            continue;

                        for (int j = 0; j < requestsToPush; j++) {
                            final int actorId = requests[i + j];

                            executorService.submit(new Runnable() {
                                public void run() {

                                    // here we emulate that requests from one actor are serialized.
                                    final Object perActorLock = actorLock[actorId];

                                    synchronized (perActorLock) {
                                        BasicMemcachedClient client = null;

                                        try {
                                            // take a client from pool
                                            client = actorClients.take();

                                            // emulated main application loop:
                                            // 1. load from memcache
                                            // 2. do some actions
                                            // 3. save to memcache
                                            long startMs = System.currentTimeMillis();

                                            // read from memcache
                                            final String cacheKey = "actorId:" + actorPrefix + ":" + actorId;
                                            byte[] bytes = client.get(cacheKey);
                                            reqCount.incrementAndGet();

                                            if (!actorInitialized[actorId]) {
                                                bytes = seedBuffer;
                                                actorInitialized[actorId] = true;
                                            } else if (bytes == null || bytes.length != seedBuffer.length)
                                                throw new RuntimeException("returned null or broken data");

                                            // modify data -- skipped
                                            // write from memcache
                                            client.set(cacheKey, bytes);
                                            reqCount.incrementAndGet();

                                            transactionCount.incrementAndGet();
                                            avgRespTime.addAndGet(System.currentTimeMillis() - startMs);
                                            avgRespTimeBase.incrementAndGet();
                                        } catch (Exception e) {
                                            errorCount.incrementAndGet();
                                            //noinspection ThrowableResultOfMethodCallIgnored
                                            errorSet.putIfAbsent(e.getClass(), e);
                                        } finally {
                                            if (client != null)
                                                actorClients.offer(client);
                                            allDone.countDown();
                                        }
                                    }

                                    queueSize.decrementAndGet();
                                }
                            });
                            queueSize.incrementAndGet();
                        }

                        i += requestsToPush;
//                        System.out.println(requestsToPush + "," + enterCount.get());
                    }

                    // sleep till next tick
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        ioThread.setDaemon(true);
        ioThread.start();

        // poll until end and read counters
        int lastTransactionCount = transactionCount.get();
        int lastReqCount = reqCount.get();
        long lastAvgRespTime = avgRespTime.get();
        int lastAvgRespTimeBase = avgRespTimeBase.get();
        int lastErrorCount = errorCount.get();

        while (!allDone.await(1, TimeUnit.SECONDS)) {
            final int newTransactionCount = transactionCount.get();
            final int newReqCount = reqCount.get();
            final int newErrorCount = errorCount.get();
            final long newAvgRespTime = avgRespTime.get();
            final int newAvgRespTimeBase = avgRespTimeBase.get();
            final int dxRespTimeBase = newAvgRespTimeBase - lastAvgRespTimeBase;

            System.out.printf("tps: %5d, tt: %5d, mrps: %5d, qs: %5d, err: %5d, avtt: %d",
                    (newTransactionCount - lastTransactionCount),
                    newTransactionCount,
                    (newReqCount - lastReqCount),
                    queueSize.get(),
                    (newErrorCount - lastErrorCount),
                    dxRespTimeBase == 0 ? 0 : (newAvgRespTime - lastAvgRespTime) / dxRespTimeBase);

            System.out.println();
            lastTransactionCount = newTransactionCount;
            lastReqCount = newReqCount;
            lastErrorCount = newErrorCount;

            if (dxRespTimeBase > 0) {
                lastAvgRespTime = newAvgRespTime;
                lastAvgRespTimeBase = newAvgRespTimeBase;
            }
        }

        // report errors
        if (errorCount.get() > 0) {
            // print errors:
            System.out.println("exceptions happened:" + errorCount.get());
            for (final Exception exception : errorSet.values()) {
                System.out.println("exception type = " + exception);
            }
        } else {
            System.out.println("no errors");
        }

        System.out.println();

        // recycle resources
        executorService.shutdownNow();
        Thread.sleep(2000);
    }
}
