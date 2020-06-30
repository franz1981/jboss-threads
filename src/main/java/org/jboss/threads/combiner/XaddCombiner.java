/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jboss.threads.combiner;

import org.jctools.queues.MpscUnboundedXaddArrayQueue;

import java.util.concurrent.atomic.AtomicBoolean;

public class XaddCombiner implements Combiner {

    private static final class Operation extends AtomicBoolean implements AutoCloseable {
        private Runnable runnable;

        @Override
        public void close() throws Exception {
            runnable = null;
            lazySet(false);
        }
    }

    private final MpscUnboundedXaddArrayQueue<Operation> queue;
    private final AtomicBoolean lock;
    private static final ThreadLocal<Operation> OP_POOL = ThreadLocal.withInitial(Operation::new);

    public XaddCombiner() {
        this.queue = new MpscUnboundedXaddArrayQueue<>(Math.min(Runtime.getRuntime().availableProcessors(), 16));
        this.lock = new AtomicBoolean(false);
    }

    @Override

    public void combine(Runnable action, IdleStrategy idleStrategy) {
        final Operation operation = OP_POOL.get();
        operation.runnable = action;
        int idleCount = 0;
        for (; ; ) {
            if (lock.compareAndSet(false, true)) {
                idleCount = 0;
                try {
                    Operation op;
                    while ((op = queue.poll()) != null) {
                        try {
                            op.runnable.run();
                        } finally {
                            op.runnable = null;
                            op.lazySet(true);
                        }
                    }
                } finally {
                    lock.lazySet(false);
                }
            }
            if (operation.get()) {
                return;
            }
            idleCount = idleStrategy.idle(idleCount);
        }
    }
}
