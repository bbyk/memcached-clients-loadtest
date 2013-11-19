package bbyk.loadtests;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * @author bbyk
 */
public class ClientFactory {
    private static final int OP_TIMEOUT = 2000;
    private ClientSetup setup;
    private InetSocketAddress[] addresses;
    private BasicMemcachedClient sharedClient;
    private List<URI> couchBaseHosts;

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

        couchBaseHosts = Arrays.asList(Iterables.toArray(Iterables.transform(Arrays.asList(addresses),
                new Function<InetSocketAddress, URI>() {
                    public URI apply(InetSocketAddress address) {
                        try {
                            return new URI("http://" + address.getHostName() + ':' + address.getPort() + "/pools");
                        } catch (URISyntaxException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }), URI.class));
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
            case SHARED_ONE_SPY_COUCHBASE:
                final CouchbaseConnectionFactoryBuilder spyCouchBuilder = new CouchbaseConnectionFactoryBuilder();
                spyCouchBuilder.setTimeoutExceptionThreshold(OP_TIMEOUT);
                spyCouchBuilder.setOpTimeout(OP_TIMEOUT); // 2 sec

                // turning off compression
                final SerializingTranscoder couchTranscoder = new SerializingTranscoder();
                couchTranscoder.setCompressionThreshold(Integer.MAX_VALUE);
                spyCouchBuilder.setTranscoder(couchTranscoder);

                return new BasicMemcachedClient() {
                    final CouchbaseConnectionFactory connectionFactory = spyCouchBuilder.buildCouchbaseConnection(couchBaseHosts, "default", "", "");
                    
                    final CouchbaseClient c = new CouchbaseClient(connectionFactory);

                    public byte[] get(@NotNull String key) {
                        return (byte[]) c.get(key);
                    }

                    public void set(@NotNull String key, byte[] buffer) {
                        c.set(key, 0, buffer);
                    }
                };
                
            case SHARED_ONE_SPY_MEMCACHED:
            case PER_THREAD_SPY_MEMCACHED:
                final ConnectionFactoryBuilder spyBuilder = new ConnectionFactoryBuilder();
                spyBuilder.setTimeoutExceptionThreshold(OP_TIMEOUT);
                spyBuilder.setOpTimeout(OP_TIMEOUT); // 2 sec

                // turning off compression
                final SerializingTranscoder transcoder = new SerializingTranscoder();
                transcoder.setCompressionThreshold(Integer.MAX_VALUE);
                spyBuilder.setTranscoder(transcoder);

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
