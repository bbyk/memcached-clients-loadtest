package bbyk.loadtests;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.spy.memcached.ConnectionFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * @author bbyk
 */
public class ClientFactory {
    private static final int OP_TIMEOUT = 2000;
    private ClientSetup setup;
    private InetSocketAddress[] addresses;
    private BasicMemcachedClient sharedClient;

    public ClientFactory(@NotNull ClientSetup setup, @NotNull InetSocketAddress[] addresses) {
        this.setup = setup;
        this.addresses = addresses;

        SockIOPool pool = SockIOPool.getInstance();
        final String[] strAddresses = Iterables.toArray(Iterables.transform(Arrays.asList(addresses),
                new Function<InetSocketAddress, String>() {
                    public String apply(InetSocketAddress address) {
                        return address.getHostName() + ':' + address.getPort();
                    }
                }), String.class);
        pool.setServers(strAddresses);
        pool.setSocketTO(OP_TIMEOUT); // whalin socket timeout
        pool.initialize();
    }

    public BasicMemcachedClient getOrCreate() throws Exception {
        if (setup.isShared) {
            if (sharedClient == null)
                sharedClient = create();
            return sharedClient;
        } else {
            return create();
        }
    }

    private BasicMemcachedClient create() throws Exception {

        switch (setup) {
            default:
            case SHARED_ONE_SPY_MEMCACHED:
            case PER_THREAD_SPY_MEMCACHED:
                final ConnectionFactoryBuilder spyBuilder = new ConnectionFactoryBuilder();
                spyBuilder.setTimeoutExceptionThreshold(OP_TIMEOUT);
                spyBuilder.setOpTimeout(OP_TIMEOUT); // 2 sec

                return new BasicMemcachedClient() {
                    final net.spy.memcached.MemcachedClient c = new net.spy.memcached.MemcachedClient(spyBuilder.build(),
                            Arrays.asList(addresses));

                    public byte[] get(@NotNull String key) {
                        return (byte[]) c.get(key);
                    }

                    public void set(@NotNull String key, byte[] buffer) {
                        c.set(key, 0, buffer);
                    }
                };
            case SHARED_ONE_WHALIN:
                return new BasicMemcachedClient() {
                    final MemCachedClient c = new MemCachedClient();

                    public byte[] get(@NotNull String key) {
                        return (byte[]) c.get(key);
                    }

                    public void set(@NotNull String key, @Nullable byte[] buffer) {
                        c.set(key, buffer);
                    }
                };
            case SHARED_ONE_XMEMCACHED:
                final MemcachedClientBuilder xbuilder = new XMemcachedClientBuilder(Arrays.asList(addresses));
                xbuilder.setConnectionPoolSize(2);
                xbuilder.getConfiguration().setSoTimeout(OP_TIMEOUT); // 2 sec

                return new BasicMemcachedClient() {
                    final net.rubyeye.xmemcached.MemcachedClient c = xbuilder.build();

                    public byte[] get(@NotNull String key) {
                        try {
                            return (byte[]) c.get(key);
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }

                    public void set(@NotNull String key, @Nullable byte[] buffer) {
                        try {
                            c.set(key, 0, buffer);
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                };
        }
    }
}
