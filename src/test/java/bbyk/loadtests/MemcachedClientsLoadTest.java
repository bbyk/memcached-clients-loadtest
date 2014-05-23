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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
                /* doc size, cthreads, nthreads, actors, requests per second, seconds, heavy lifting */
                5 * 1024, 300, 300, 10000, 2000, 120, 0,
                30 * 1024, 5, 5, 10000, 1000, 1, 0,
                5 * 1024, 5, 5, 10000, 1000, 120, 0,
                5 * 1024, 50, 50, 10000, 1000, 120, 0,
                30 * 1024, 100, 100, 10000, 1000, 120, 0,
                5 * 1024, 100, 100, 10000, 1000, 120, 0
        };
        final ClientSetup[] clientSetups = new ClientSetup[]{ClientSetup.SHARED_ONE_SPY_MEMCACHED, ClientSetup.SHARED_ONE_WHALIN};

        System.out.println("tps - transactions per second");
        System.out.println("tt - total number transactions processed");
        System.out.println("mrps - memcached requests per second");
        System.out.println("qs - requests queued to process");
        System.out.println("err - number of errors");
        System.out.println("avtt - avarage transaction time in milliseconds");
        System.out.println("mtt - maximum transaction time in milliseconds");
        System.out.println("avct - avarage memcache/couchbase call time in milliseconds");
        System.out.println("busy - number of busy threads");
        System.out.println("qst - thread pool queue size");
        System.out.println();

        for (final ClientSetup clientSetup : clientSetups) {
            for (int i = 0; i < params.length; ) {
                testReadWriteProfileSafe(clientSetup, params[i++], params[i++], params[i++], params[i++], params[i++], params[i++], params[i++] == 1);
            }
        }
    }

    private void testReadWriteProfileSafe(@NotNull final ClientSetup setup, final int docSize, final int minThreadCount, final int maxThreadCount,
                                          final int numberOfActors, final int requestsPerSecond,
                                          final int seconds, final boolean doHeavyLifting) throws Exception {
        try {
            testReadWriteProfile(setup, docSize, minThreadCount, maxThreadCount, numberOfActors, requestsPerSecond, seconds, doHeavyLifting);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testReadWriteProfile(@NotNull final ClientSetup setup, final int docSize, final int minThreadCount, final int maxThreadCount,
                                      final int numberOfActors, final int requestsPerSecond,
                                      final int seconds, final boolean doHeavyLifting) throws Exception {
        System.out.printf("Setup: %s, document size: %d, threadCount: %d, numberOfActors: %d, requestsPerSecond: %d, seconds: %d",
                setup,
                docSize,
                maxThreadCount,
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
        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(minThreadCount, maxThreadCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        final CountDownLatch allDone = new CountDownLatch(totalNumberOfRequests);
        final String actorPrefix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
        final AtomicInteger errorCount = new AtomicInteger();
        final ConcurrentHashMap<Class, Exception> errorSet = new ConcurrentHashMap<Class, Exception>();
        final ClientFactory clientFactory = new ClientFactory(setup, addresses);
        final AtomicInteger transactionCount = new AtomicInteger();
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicLong avgCallRespTime = new AtomicLong();
        final AtomicLong avgTransTime = new AtomicLong();
        final AtomicLong maxTransTime = new AtomicLong();
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

                                            long startCallMs = System.currentTimeMillis();
                                            byte[] bytes = client.get(cacheKey);
                                            avgCallRespTime.addAndGet(System.currentTimeMillis() - startCallMs);
                                            callCount.incrementAndGet();

                                            if (!actorInitialized[actorId]) {
                                                bytes = seedBuffer.clone();
                                                actorInitialized[actorId] = true;
                                            } else if (bytes == null || bytes.length != seedBuffer.length)
                                                throw new RuntimeException("returned null or broken data");

                                            if (doHeavyLifting)
                                                doHeavyLifting(bytes);
                                            // modify data -- skipped
                                            // write from memcache
                                            startCallMs = System.currentTimeMillis();
                                            client.set(cacheKey, bytes);
                                            avgCallRespTime.addAndGet(System.currentTimeMillis() - startCallMs);
                                            callCount.incrementAndGet();

                                            final long delta = System.currentTimeMillis() - startMs;
                                            avgTransTime.addAndGet(delta);

                                            for (;;) {
                                                long current = maxTransTime.get();
                                                if (current < delta) {
                                                    if (!maxTransTime.compareAndSet(current, delta))
                                                        continue;
                                                }

                                                break;
                                            }
                                            transactionCount.incrementAndGet();
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
        int lastCallCount = callCount.get();
        long lastAvgTransTime = avgTransTime.get();
        long lastAvgCallRespTime = avgCallRespTime.get();
        int lastErrorCount = errorCount.get();

        while (!allDone.await(1, TimeUnit.SECONDS)) {
            final int newTransactionCount = transactionCount.get();
            final int newCallCount = callCount.get();
            final int newErrorCount = errorCount.get();
            final long newAvgTransTime = avgTransTime.get();
            final long newAvgCallRespTime = avgCallRespTime.get();
            final int dxTransactionCount = newTransactionCount - lastTransactionCount;
            final int dxCallCount = newCallCount - lastCallCount;
            final long copyMaxTransTime = maxTransTime.get();
            maxTransTime.set(0);

            System.out.printf("tps: %5d, tt: %5d, mrps: %5d, qs: %5d, err: %5d, avtt: %d, mtt: %d, avct: %d, busy: %d, qst: %d",
                    dxTransactionCount,
                    newTransactionCount,
                    dxCallCount,
                    queueSize.get(),
                    (newErrorCount - lastErrorCount),
                    dxTransactionCount == 0 ? 0 : (newAvgTransTime - lastAvgTransTime) / dxTransactionCount,
                    copyMaxTransTime,
                    dxCallCount == 0 ? 0 : (newAvgCallRespTime - lastAvgCallRespTime) / dxCallCount,
                    executorService.getActiveCount(),
                    executorService.getQueue().size());

            System.out.println();
            lastTransactionCount = newTransactionCount;
            lastCallCount = newCallCount;
            lastErrorCount = newErrorCount;

            if (dxTransactionCount > 0) {
                lastAvgTransTime = newAvgTransTime;
                lastAvgCallRespTime = newAvgCallRespTime;
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

    private void doHeavyLifting(byte[] bytes) {
        final Random rnd = new Random();
        for (int ii = 0; ii < Math.min(bytes.length, 500); ii++)
            for (int jj = ii; jj < Math.min(bytes.length, 500); jj++) {
                byte tmp = bytes[ii];
                bytes[ii] = (byte) ((bytes[jj] * rnd.nextInt()) % Byte.MAX_VALUE);
                bytes[jj] = tmp;
            }
    }
}
