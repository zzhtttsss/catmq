package org.catmq.storage.messageLog;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.catmq.constant.FileConstant;
import org.catmq.thread.ServiceThread;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

/**
 * Service thread to allocate {@link MessageLog}.
 */
@Slf4j
public class AllocateMessageLogService extends ServiceThread {

    private final ConcurrentHashMap<String, AllocateRequest> requestMap = new ConcurrentHashMap<>();

    private final PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<>();

    private volatile boolean hasException = false;

    private static final int waitTimeOut = 1000 * 5;

    /**
     * Get the next {@link MessageLog}.
     * When this method be call, it also makes a request to let the service thread create the next
     * {@link MessageLog} of the next {@link MessageLog}. In this way, we can prepare the next
     * {@link MessageLog} in advance so that write threads can get the next {@link MessageLog}
     * immediately.
     *
     * @param nextFilePath     the path of the next message log file.
     * @param nextNextFilePath the path of the next message log file of the next message log file.
     * @param fileSize         the size of message log file.
     * @return The next {@link MessageLog}
     */
    public MessageLog getNextMessageLog(String nextFilePath, String nextNextFilePath, int fileSize) {
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        if (this.requestMap.putIfAbsent(nextFilePath, nextReq) == null) {
            this.requestQueue.offer(nextReq);
        }
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        if (this.requestMap.putIfAbsent(nextNextFilePath, nextNextReq) == null) {
            this.requestQueue.offer(nextNextReq);
        }

        if (hasException) {
            log.warn("{} service has exception. so return null", this.getServiceName());
            return null;
        }

        AllocateRequest result = this.requestMap.get(nextFilePath);
        try {
            if (result != null) {
                // Wait for the new messageLog to be created.
                boolean ok = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!ok) {
                    log.warn("Create mmap timeout, file path: {}, file size: {}", result.getFilePath(), result.getFileSize());
                    return null;
                } else {
                    this.requestMap.remove(nextFilePath);
                    return result.getMessageLog();
                }
            }
        } catch (InterruptedException e) {
            log.warn("{} service has exception.", this.getServiceName(), e);
        }

        return null;
    }

    /**
     * Create a {@link MessageLog}.
     * If turn on file warm up, it will let the OS load the message log file into virtual memory and
     * lock it. File warm up can significantly increase the speed of writing the message log file,
     * but it will take some time to be done.
     *
     * @return whether the service need continue running.
     */
    private boolean createMessageLog() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestMap.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("This mmap request expired, maybe cause timeout, file path: {}, file size: {}",
                        req.getFilePath(), req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("Never expected here,  maybe cause timeout, file path: {}, file size: {}, expected request: {}",
                        req.getFilePath(), req.getFileSize(), expectedRequest);
                return true;
            }

            if (req.getMessageLog() == null) {
                MessageLog messageLog = new MessageLog(req.getFilePath(), req.getFileSize());
                // preheating the messageLog
                if (STORER_CONFIG.isNeedWarmMappedFile()) {
                    messageLog.warmUpMappedFile();
                }

                req.setMessageLog(messageLog);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (req != null) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess) {
                req.getCountDownLatch().countDown();
            }
        }
        return true;

    }

    @Override
    public String getServiceName() {
        return AllocateMessageLogService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());

        while (!this.isStopped() && this.createMessageLog()) {

        }
        log.info("{} service end.", this.getServiceName());
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestMap.values()) {
            if (req.messageLog != null) {
                log.info("Delete pre allocated mapped file, {}", req.messageLog.getFileName());
                // TODO 销毁逻辑
//                req.messageLog.destroy(1000);
            }
        }
    }

    /**
     * Represent a request to get a new messageLog.
     */
    @Data
    static class AllocateRequest implements Comparable<AllocateRequest> {

        private static final int countDownLatchCount = 1;
        // Full file path
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(countDownLatchCount);
        private volatile MessageLog messageLog = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        /**
         * First compare the file size, then compare the file offset.
         */
        @Override
        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize) {
                return 1;
            } else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(FileConstant.LEFT_SLASH);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(FileConstant.LEFT_SLASH);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                return Long.compare(mName, oName);
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null) {
                    return false;
                }
            } else if (!filePath.equals(other.filePath)) {
                return false;
            }
            if (fileSize != other.fileSize) {
                return false;
            }
            return true;
        }
    }
}
