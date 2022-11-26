package org.catmq.remoting.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author BYL
 */
@Data
@AllArgsConstructor
public class Pair<T1, T2> {
    private T1 object1;
    private T2 object2;

}
