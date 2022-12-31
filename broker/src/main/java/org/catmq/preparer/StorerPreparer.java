package org.catmq.preparer;

import org.catmq.context.RequestContext;

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
