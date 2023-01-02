package org.catmq.preparer;

import lombok.extern.slf4j.Slf4j;
import org.catmq.context.RequestContext;

@Slf4j
public class StorerPreparer implements Preparer {
    @Override
    public void prepare(RequestContext ctx) {
    }

    public enum StorerPreparerEnum {
        INSTANCE;
        private final StorerPreparer storerPreparer;
        StorerPreparerEnum() {
            storerPreparer = new StorerPreparer();
        }
        public StorerPreparer getInstance() {
            return storerPreparer;
        }
    }
}
