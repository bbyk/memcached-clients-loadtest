package bbyk.loadtests;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import net.spy.memcached.MemcachedClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * @author bbyk
 */
public class ClientFactory {
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
        pool.initialize();
    }

    public BasicMemcachedClient getOrCreate() throws Exception {
        switch (setup) {
            default:
            case SHARED_ONE_WHALIN:
            case SHARED_ONE_SPY_MEMCACHED:
                if (sharedClient == null)
                    sharedClient = create();
                return sharedClient;
            case PER_THREAD_SPY_MEMCACHED:
                return create();
        }
    }

    private BasicMemcachedClient create() throws Exception {

        switch (setup) {
            default:
            case SHARED_ONE_SPY_MEMCACHED:
            case PER_THREAD_SPY_MEMCACHED:
                return new BasicMemcachedClient() {
                    final MemcachedClient c = new MemcachedClient(addresses);

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

        }
    }
}
