package org.catmq.storage.messageLog;

import lombok.extern.slf4j.Slf4j;
import org.catmq.util.StringUtil;

import java.io.File;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.catmq.constant.FileConstant.GB;
import static org.catmq.constant.FileConstant.MB;

@Slf4j
public class MessageLogStorage {

    public final int maxMessageLogSize;

    public final String path;

    public final CopyOnWriteArrayList<MessageLog> messageLogs = new CopyOnWriteArrayList<>();

    public final AllocateMessageLogService allocateMessageLogService;

    public MessageLogStorage() {
        // TODO read config
        this.path = "/Users/zzh/Documents/projects/catmq/catmq/storer/src/messageLog";
        this.maxMessageLogSize = 1 * (int) MB;
        allocateMessageLogService = new AllocateMessageLogService();
        allocateMessageLogService.start();
        log.info("start to create");
        this.tryCreateMessageLog(0);
    }

    public MessageLog getLatestMessageLog() {
        return messageLogs.get(messageLogs.size() - 1);
    }

    public MessageLog getLastMessageLog(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MessageLog mappedFileLast = getLastMessageLog();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.maxMessageLogSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getOffset() + this.maxMessageLogSize;
        }
        log.debug("createOffset is {}", createOffset);
        if (createOffset != -1 && needCreate) {
            return tryCreateMessageLog(createOffset);
        }

        return mappedFileLast;
    }

    public MessageLog tryCreateMessageLog(long createOffset) {
        String nextFilePath = StringUtil.concatString(this.path, File.separator, StringUtil.offset2FileName(createOffset));
        String nextNextFilePath = StringUtil.concatString(this.path, File.separator,
                StringUtil.offset2FileName(createOffset + this.maxMessageLogSize));
        log.debug("nextFilePath is {}, nextNextFilePath is {}", nextFilePath, nextNextFilePath);
        MessageLog messageLog = this.allocateMessageLogService.getNextMessageLog(nextFilePath,
                nextNextFilePath, this.maxMessageLogSize);
        if (messageLog != null) {
            log.debug("messageLog is not null.");
            this.messageLogs.add(messageLog);
        }
        return messageLog;
    }

    public MessageLog getLastMessageLog() {
        MessageLog[] messageLogs = this.messageLogs.toArray(new MessageLog[0]);
        return messageLogs.length == 0 ? null : messageLogs[messageLogs.length - 1];
    }

    public MessageLog getLastMessageLog(final long startOffset) {
        return getLastMessageLog(startOffset, true);
    }
}
