package org.catmq.collection;

import io.netty.util.Recycler.Handle;
import java.util.ArrayList;

/**
 * A simple list which is recyclable.
 */
public final class RecyclableArrayList<T> extends ArrayList<T> {

    private static final int DEFAULT_INITIAL_CAPACITY = 8;

    /**
     * An ArrayList recycler.
     */
    public static class Recycler<X>
            extends io.netty.util.Recycler<RecyclableArrayList<X>> {
        @Override
        protected RecyclableArrayList<X> newObject(
                Handle<RecyclableArrayList<X>> handle) {
            return new RecyclableArrayList<X>(handle, DEFAULT_INITIAL_CAPACITY);
        }

        public RecyclableArrayList<X> newInstance() {
            return get();
        }
    }

    private final Handle<RecyclableArrayList<T>> handle;

    /**
     * Default non-pooled instance.
     */
    public RecyclableArrayList() {
        super();
        this.handle = null;
    }

    private RecyclableArrayList(Handle<RecyclableArrayList<T>> handle, int initialCapacity) {
        super(initialCapacity);
        this.handle = handle;
    }

    public void recycle() {
        clear();
        if (handle != null) {
            handle.recycle(this);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

}
