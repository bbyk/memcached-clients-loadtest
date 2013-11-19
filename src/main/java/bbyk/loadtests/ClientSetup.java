package bbyk.loadtests;

/**
 * @author bbyk
 */
public enum ClientSetup {
    SHARED_ONE_SPY_COUCHBASE(true),
    SHARED_ONE_SPY_MEMCACHED(true),
    SHARED_ONE_WHALIN(true),
    SHARED_ONE_XMEMCACHED(true),
    PER_THREAD_SPY_MEMCACHED(false);

    boolean isShared;

    ClientSetup(boolean shared) {
        this.isShared = shared;
    }
}
