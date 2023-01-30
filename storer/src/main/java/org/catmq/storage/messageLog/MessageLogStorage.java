package org.catmq.storage.messageLog;

import lombok.extern.slf4j.Slf4j;
import org.catmq.constant.FileConstant;
import org.catmq.util.StringUtil;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;
import static org.catmq.storer.Storer.STORER;

/**
 * Manage all {@link MessageLog}.
 */
@Slf4j
public class MessageLogStorage {

    public final int maxMessageLogSize;
    public final String path;
    public final CopyOnWriteArrayList<MessageLog> messageLogs = new CopyOnWriteArrayList<>();
    public final AllocateMessageLogService allocateMessageLogService;

    private MessageLogStorage() {
        this.path = STORER_CONFIG.getMessageLogStoragePath();
        this.maxMessageLogSize = STORER_CONFIG.getMessageLogMaxFileSize();
        allocateMessageLogService = new AllocateMessageLogService();
        allocateMessageLogService.start();
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
        if (createOffset != -1 && needCreate) {
            return tryCreateMessageLog(createOffset);
        }
        return mappedFileLast;
    }

    /**
     * Get a new {@link MessageLog} from the {@link AllocateMessageLogService}.
     *
     * @param createOffset the beginning offset of the new {@link MessageLog}
     * @return a new {@link MessageLog}
     */
    public MessageLog tryCreateMessageLog(long createOffset) {
        String nextFilePath = StringUtil.concatString(this.path, FileConstant.LEFT_SLASH,
                StringUtil.offset2FileName(createOffset));
        String nextNextFilePath = StringUtil.concatString(this.path, FileConstant.LEFT_SLASH,
                StringUtil.offset2FileName(createOffset + this.maxMessageLogSize));
        MessageLog messageLog = this.allocateMessageLogService.getNextMessageLog(nextFilePath,
                nextNextFilePath, this.maxMessageLogSize);
        if (messageLog != null) {
            this.messageLogs.add(messageLog);
            STORER.getStorerInfo().addMessageLogNum();

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

    public int getMessageLogNum() {
        return messageLogs.size();
    }

    public enum MessageLogStorageEnum {
        INSTANCE;
        private final MessageLogStorage messageLogStorage;

        MessageLogStorageEnum() {
            messageLogStorage = new MessageLogStorage();
        }

        public MessageLogStorage getInstance() {
            return messageLogStorage;
        }
    }
}
