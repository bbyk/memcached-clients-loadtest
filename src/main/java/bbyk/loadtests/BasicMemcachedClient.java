package bbyk.loadtests;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author bbyk
 */
public interface BasicMemcachedClient {
    byte[] get(@NotNull String key);

    void set(@NotNull String key, @Nullable byte[] buffer);
}
