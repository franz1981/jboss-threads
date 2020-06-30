package org.jboss.threads;

import org.jboss.logging.Logger;
import org.wildfly.common.Assert;
import org.wildfly.common.lock.ExtendedLock;
import org.wildfly.common.lock.Locks;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * An executor service that is actually a "view" over another executor service.
 */
final class QueuedViewExecutor extends ViewExecutor {
    private static final Logger log = Logger.getLogger("org.jboss.threads.view-executor");
    private static final Runnable[] NO_RUNNABLES = new Runnable[0];

    private final Executor delegate;

    private final ExtendedLock lock;
    private final Condition shutDownCondition;
    private final ArrayDeque<Runnable> queue;
    private final Set<TaskWrapper> allWrappers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final int queueLimit;
    private final short maxCount;
    private short submittedCount;
    private short runningCount;
    private int state = ST_RUNNING;

    private static final int ST_RUNNING = 0;
    private static final int ST_SHUTDOWN_REQ = 1;
    private static final int ST_SHUTDOWN_INT_REQ = 2;
    private static final int ST_STOPPED = 3;

    QueuedViewExecutor(
            final Executor delegate,
            final short maxCount,
            final int queueLimit,
            final int queueInitialSize,
            final Thread.UncaughtExceptionHandler handler) {
        this.delegate = delegate;
        this.maxCount = maxCount;
        this.queueLimit = queueLimit;
        this.setExceptionHandler(handler);
        queue = new ArrayDeque<>(Math.min(queueLimit, queueInitialSize));
        lock = Locks.reentrantLock();
        shutDownCondition = lock.newCondition();
    }

    public void execute(final Runnable command) {
        lock.lock();
        try {
            if (state != ST_RUNNING) {
                throw new RejectedExecutionException("Executor has been shut down");
            }
            final short submittedCount = this.submittedCount;
            if (runningCount + submittedCount < maxCount) {
                this.submittedCount = (short) (submittedCount + 1);
                final TaskWrapper tw = new TaskWrapper(JBossExecutors.classLoaderPreservingTask(command));
                allWrappers.add(tw);
                try {
                    /* this cannot be easily moved outside of the lock, otherwise queued tasks might never run
                     * under certain rare scenarios.
                     */
                    delegate.execute(tw);
                } catch (Throwable t) {
                    this.submittedCount--;
                    allWrappers.remove(tw);
                    throw t;
                }
            } else if (queue.size() < queueLimit) {
                queue.add(command);
            } else {
                throw new RejectedExecutionException("No executor queue space remaining");
            }
        } finally {
            lock.unlock();
        }
    }

    public void shutdown(boolean interrupt) {
        lock.lock();
        int oldState = this.state;
        if (oldState < ST_SHUTDOWN_REQ) {
            // do shutdown
            final boolean emptyQueue;
            try {
                emptyQueue = queue.isEmpty();
            } catch (Throwable t) {
                lock.unlock();
                throw t;
            }
            if (runningCount == 0 && submittedCount == 0 && emptyQueue) {
                this.state = ST_STOPPED;
                try {
                    shutDownCondition.signalAll();
                } finally {
                    lock.unlock();
                }
                runTermination();
                return;
            }
        }
        // didn't exit
        this.state = interrupt ? ST_SHUTDOWN_INT_REQ : ST_SHUTDOWN_REQ;
        lock.unlock();
        if (interrupt && oldState < ST_SHUTDOWN_INT_REQ) {
            // interrupt all runners
            for (TaskWrapper wrapper : allWrappers) {
                wrapper.interrupt();
            }
        }
    }

    public List<Runnable> shutdownNow() {
        lock.lock();
        int oldState = this.state;
        final Runnable[] tasks;
        try {
            tasks = queue.toArray(NO_RUNNABLES);
            queue.clear();
        } catch (Throwable t) {
            lock.unlock();
            throw t;
        }
        if (oldState < ST_SHUTDOWN_INT_REQ) {
            // do shutdown
            if (runningCount == 0 && submittedCount == 0) {
                this.state = ST_STOPPED;
                try {
                    shutDownCondition.signalAll();
                } finally {
                    lock.unlock();
                }
                runTermination();
            } else {
                lock.unlock();
                this.state = ST_SHUTDOWN_INT_REQ;
                // interrupt all runners
                for (TaskWrapper wrapper : allWrappers) {
                    wrapper.interrupt();
                }
            }
        } else {
            lock.unlock();
        }
        return Arrays.asList(tasks);
    }

    public boolean isShutdown() {
        lock.lock();
        try {
            return state >= ST_SHUTDOWN_REQ;
        } finally {
            lock.unlock();
        }
    }

    public boolean isTerminated() {
        lock.lock();
        try {
            return state == ST_STOPPED;
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            if (state == ST_STOPPED) {
                return true;
            }
            final long nanos = unit.toNanos(timeout);
            if (nanos <= 0) {
                return false;
            }
            long elapsed = 0;
            final long start = System.nanoTime();
            for (;;) {
                shutDownCondition.awaitNanos(nanos - elapsed);
                if (state == ST_STOPPED) {
                    return true;
                }
                elapsed = System.nanoTime() - start;
                if (elapsed >= nanos) {
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        return "view of " + delegate;
    }

    class TaskWrapper implements Runnable {
        private volatile Thread thread;
        private Runnable command;

        TaskWrapper(final Runnable command) {
            this.command = command;
        }

        void interrupt() {
            final Thread thread = this.thread;
            if (thread != null) {
                thread.interrupt();
            }
        }

        public void run() {
            boolean removeFromAllWrappersAfterCompletion = true;
            thread = Thread.currentThread();
            try {
                for (;;) {
                    lock.lock();
                    try {
                        submittedCount--;
                        runningCount++;
                    } finally {
                        lock.unlock();
                    }
                    try {
                        command.run();
                    } catch (Throwable t) {
                        try {
                            getExceptionHandler().uncaughtException(Thread.currentThread(), t);
                        } catch (Throwable ignored) {
                        }
                    }
                    lock.lock();
                    runningCount--;
                    try {
                        command = queue.pollFirst();
                    } catch (Throwable t) {
                        lock.unlock();
                        throw t;
                    }
                    if (runningCount + submittedCount < maxCount && command != null) {
                        // execute next
                        submittedCount++;
                        lock.unlock();
                    } else if (command == null && runningCount == 0 && submittedCount == 0 && state != ST_RUNNING) {
                        // we're the last task
                        state = ST_STOPPED;
                        try {
                            shutDownCondition.signalAll();
                        } finally {
                            lock.unlock();
                        }
                        runTermination();
                        return;
                    } else {
                        lock.unlock();
                        return;
                    }
                    try {
                        delegate.execute(this);
                        removeFromAllWrappersAfterCompletion = false;
                        // resubmitted this task for execution, so return
                        return;
                    } catch (Throwable t) {
                        log.warn("Failed to resubmit executor task to delegate executor"
                            + " (executing task immediately instead)", t);
                        // resubmit failed, so continue execution in this thread
                    }
                }
            } finally {
                if (removeFromAllWrappersAfterCompletion) {
                    allWrappers.remove(this);
                }
                thread = null;
            }
        }
    }
}
