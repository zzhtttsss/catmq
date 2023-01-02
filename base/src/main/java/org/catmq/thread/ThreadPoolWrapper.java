package org.catmq.thread;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

@Getter
@Setter
public class ThreadPoolWrapper {
    private String name;
    private ThreadPoolExecutor threadPoolExecutor;
    private List<ThreadPoolStatusMonitor> statusPrinters;

    ThreadPoolWrapper(final String name, final ThreadPoolExecutor threadPoolExecutor,
                      final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.name = name;
        this.threadPoolExecutor = threadPoolExecutor;
        this.statusPrinters = statusPrinters;
    }

    public static class ThreadPoolWrapperBuilder {
        private String name;
        private ThreadPoolExecutor threadPoolExecutor;
        private List<ThreadPoolStatusMonitor> statusPrinters;

        ThreadPoolWrapperBuilder() {
        }

        public ThreadPoolWrapper.ThreadPoolWrapperBuilder name(final String name) {
            this.name = name;
            return this;
        }

        public ThreadPoolWrapper.ThreadPoolWrapperBuilder threadPoolExecutor(
                final ThreadPoolExecutor threadPoolExecutor) {
            this.threadPoolExecutor = threadPoolExecutor;
            return this;
        }

        public ThreadPoolWrapper.ThreadPoolWrapperBuilder statusPrinters(
                final List<ThreadPoolStatusMonitor> statusPrinters) {
            this.statusPrinters = statusPrinters;
            return this;
        }

        public ThreadPoolWrapper build() {
            return new ThreadPoolWrapper(this.name, this.threadPoolExecutor, this.statusPrinters);
        }

        @java.lang.Override
        public java.lang.String toString() {
            return "ThreadPoolWrapper.ThreadPoolWrapperBuilder(name=" + this.name + ", threadPoolExecutor=" + this.threadPoolExecutor + ", statusPrinters=" + this.statusPrinters + ")";
        }
    }

    public static ThreadPoolWrapper.ThreadPoolWrapperBuilder builder() {
        return new ThreadPoolWrapper.ThreadPoolWrapperBuilder();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThreadPoolWrapper wrapper = (ThreadPoolWrapper) o;
        return Objects.equal(name, wrapper.name) && Objects.equal(threadPoolExecutor, wrapper.threadPoolExecutor) && Objects.equal(statusPrinters, wrapper.statusPrinters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, threadPoolExecutor, statusPrinters);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("threadPoolExecutor", threadPoolExecutor)
                .add("statusPrinters", statusPrinters)
                .toString();
    }
}
