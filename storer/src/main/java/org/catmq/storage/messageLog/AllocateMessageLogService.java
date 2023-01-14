package org.catmq.storage.messageLog;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.ServiceThread;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AllocateMessageLogService extends ServiceThread {

    private final ConcurrentHashMap<String, AllocateRequest> requestMap = new ConcurrentHashMap<>();

    private final PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<>();

    private volatile boolean hasException = false;

    private static final int waitTimeOut = 1000 * 5;

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
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestMap.get(nextFilePath);
        try {
            if (result != null) {
                boolean ok = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!ok) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    this.requestMap.remove(nextFilePath);
                    return result.getMessageLog();
                }
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    private boolean createNewMessageLog() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestMap.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMessageLog() == null) {
                MessageLog messageLog = new MessageLog(req.getFilePath(), req.getFileSize());
                // pre write messageLog
                if (true) {
                    messageLog.warmMappedFile();
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
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                req.getCountDownLatch().countDown();
        }
        return true;

    }

    @Override
    public String getServiceName() {
        return AllocateMessageLogService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.createNewMessageLog()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestMap.values()) {
            if (req.messageLog != null) {
                log.info("delete pre allocated maped file, {}", req.messageLog.getFileName());
//                req.messageLog.destroy(1000);
            }
        }
    }

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
         * fileSize大的优先级高，文件大小相同，文件的offset越小优先级越高
         */
        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
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
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
