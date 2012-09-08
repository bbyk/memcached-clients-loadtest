package bbyk.loadtests;

import net.spy.memcached.MemcachedClient;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;

/**
 * @author bbyk
 */
public class ClientFactory {
    private ClientSetup setup;
    private InetSocketAddress address;
    private BasicMemcachedClient sharedClient;

    public ClientFactory(@NotNull ClientSetup setup, @NotNull InetSocketAddress address) {
        this.setup = setup;
        this.address = address;
    }

    public BasicMemcachedClient getOrCreate() throws Exception {
        switch (setup) {
            default:
            case SHARED_ONE_SPY_MEMCACHED:
                if (sharedClient == null)
                    sharedClient = create();
                return sharedClient;
            case PER_THREAD_SPY_MEMCACHED:
                return create();
        }
    }

    private BasicMemcachedClient create() throws Exception {
        return new BasicMemcachedClient() {
            final MemcachedClient c = new MemcachedClient(address);

            public byte[] get(@NotNull String key) {
                return (byte[]) c.get(key);
            }

            public void set(@NotNull String key, byte[] buffer) {
                c.set(key, 0, buffer);
            }
        };
    }
}
