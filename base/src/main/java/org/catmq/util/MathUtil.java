package org.catmq.util;

import java.util.concurrent.TimeUnit;

public class MathUtil {

    public static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    public static int signSafeMod(long dividend, int divisor) {
        int mod = (int) (dividend % divisor);

        if (mod < 0) {
            mod += divisor;
        }

        return mod;
    }

    /**
     * Current time from some arbitrary time base in the past, counting in
     * nanoseconds, and not affected by settimeofday or similar system clock
     * changes. This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     *
     * <p>NOTE: only use it for measuring.
     * http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#nanoTime%28%29
     *
     * @return current time in nanoseconds.
     */
    public static long nowInNano() {
        return System.nanoTime();
    }

    /**
     * Microseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time.
     *
     * @param startNanoTime the start of the interval that we are measuring
     * @return elapsed time in microseconds.
     */
    public static long elapsedMicroSec(long startNanoTime) {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanoTime);
    }
}
