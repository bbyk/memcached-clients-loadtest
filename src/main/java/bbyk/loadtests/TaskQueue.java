package bbyk.loadtests;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author bbyk
 */
public class TaskQueue extends LinkedBlockingQueue<Runnable> {
    private ThreadPoolExecutor parent;
    private final AtomicInteger allot = new AtomicInteger(0);

    public TaskQueue() {
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    monitorThings();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public boolean offer(@NotNull Runnable runnable) {
        if (parent == null) return super.offer(runnable);

        //we are maxed out on threads, simply queue the object
        if (parent.getPoolSize() == parent.getMaximumPoolSize()) return super.offer(runnable);

        synchronized (allot) {
            if (allot.get() > 0) {
                allot.decrementAndGet();
                return false;
            }
        }
        return super.offer(runnable);
    }

    public void setParent(ThreadPoolExecutor value) {
        this.parent = value;
    }

    private void monitorThings() throws Exception {
        while (true) {
            Thread.sleep(100);

            final long start = System.currentTimeMillis();
            Thread.sleep(1);
            final long took = System.currentTimeMillis() - start;

            if (took < 10 && size() > 2000) {
                synchronized (allot) {
                    if (allot.get() == 0)
                        allot.addAndGet(10);
                }
            }
        }
    }
}
