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
    private InetSocketAddress[] addresses;
    final ExecutorService executorService = Executors.newFixedThreadPool(200);


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
                /* doc size, nthreads, iterations */
                30 * 1024, 5, 1000,
                5 * 1024, 5, 1000,
                30 * 1024, 50, 1000,
                5 * 1024, 50, 1000,
                30 * 1024, 100, 1000,
                5 * 1024, 100, 1000
        };

        System.out.println("tps - transactions per second");
        System.out.println("mrps - memcached requests per second");
        System.out.println("avtt - avarage transaction time in milliseconds");
        System.out.println();

        final ClientSetup[] clientSetups = ClientSetup.values();
        for (final ClientSetup clientSetup : clientSetups) {
            for (int i = 0; i < params.length; ) {
                testReadWriteProfileSafe(clientSetup, params[i++], params[i++], params[i++]);
            }
        }
    }

    private void testReadWriteProfileSafe(@NotNull final ClientSetup setup, final int docSize, final int threadCount,
                                          final int sessionLoopCount) throws Exception {
        try {
            testReadWriteProfile(setup, docSize, threadCount, sessionLoopCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testReadWriteProfile(@NotNull final ClientSetup setup, final int docSize, final int threadCount,
                                      final int sessionLoopCount) throws Exception {
        System.out.printf("Setup: %s, document size: %d, threadCount: %d, sessionLoopCount: %s", setup, docSize,
                threadCount, sessionLoopCount);
        System.out.println();

        // prepare the seedBuffer
        final Random rnd = new Random();
        final byte[] seedBuffer = new byte[docSize];
        rnd.nextBytes(seedBuffer);

        // prepare shared state
        final CountDownLatch allDone = new CountDownLatch(threadCount);
        final String actorPrefix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
        final AtomicInteger errorCount = new AtomicInteger();
        final ConcurrentHashMap<Class, Exception> errorSet = new ConcurrentHashMap<Class, Exception>();
        final ClientFactory clientFactory = new ClientFactory(setup, addresses);
        final AtomicInteger loopCount = new AtomicInteger();
        final AtomicInteger reqCount = new AtomicInteger();
        final AtomicLong avgRespTime = new AtomicLong();
        final AtomicInteger avgRespTimeBase = new AtomicInteger();

        // prepare workers
        for (int i = 0; i < threadCount; i++) {
            // per thread state
            final int actorId = i;
            final BasicMemcachedClient client = clientFactory.getOrCreate();

            executorService.submit(new Runnable() {
                public void run() {
                    try {
                        // emulated main application loop:
                        // 1. load from memcache
                        // 2. do some actions
                        // 3. save to memcache
                        for (int j = 0; j < sessionLoopCount; j++) {
                            long startMs = System.currentTimeMillis();

                            // read from memcache
                            final String cacheKey = "actorId:" + actorPrefix + ":" + actorId;
                            byte[] bytes = client.get(cacheKey);
                            reqCount.incrementAndGet();

                            if (j == 0)
                                bytes = seedBuffer;
                            else if (bytes == null || bytes.length != seedBuffer.length)
                                throw new RuntimeException("returned null or broken data");

                            // modify data -- skipped
                            // write from memcache
                            client.set(cacheKey, bytes);
                            reqCount.incrementAndGet();

                            loopCount.incrementAndGet();
                            avgRespTime.addAndGet(System.currentTimeMillis() - startMs);
                            avgRespTimeBase.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        //noinspection ThrowableResultOfMethodCallIgnored
                        errorSet.putIfAbsent(e.getClass(), e);
                    } finally {
                        allDone.countDown();
                    }
                }
            });
        }

        // poll until end and read counters
        int lastLoopCount = loopCount.get();
        int lastReqCount = reqCount.get();
        long lastAvgRespTime = avgRespTime.get();
        int lastAvgRespTimeBase = avgRespTimeBase.get();

        while (!allDone.await(1, TimeUnit.SECONDS)) {
            final int newLoopCount = loopCount.get();
            final int newReqCount = reqCount.get();
            final long newAvgRespTime = avgRespTime.get();
            final int newAvgRespTimeBase = avgRespTimeBase.get();
            final int dxRespTimeBase = newAvgRespTimeBase - lastAvgRespTimeBase;

            System.out.printf("tps: %5d, mrps: %5d, avtt: %d",
                    (newLoopCount - lastLoopCount),
                    (newReqCount - lastReqCount),
                    dxRespTimeBase == 0 ? 0 : (newAvgRespTime - lastAvgRespTime) / dxRespTimeBase);

            System.out.println();
            lastLoopCount = newLoopCount;
            lastReqCount = newReqCount;

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
    }
}
