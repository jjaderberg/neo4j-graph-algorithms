package org.neo4j.graphalgo.core.utils.paged;

public final class PageUtil {

    // 32 KB page size
    private static final int PAGE_SIZE_IN_BYTES = 1 << 15;

    public static int pageSizeFor(int sizeOfElement) {
        assert BitUtil.isPowerOfTwo(sizeOfElement);
        return PAGE_SIZE_IN_BYTES >> Integer.numberOfTrailingZeros(sizeOfElement);
    }

    static int numPagesFor(long capacity, int pageSize) {
        int pageShift = Integer.numberOfTrailingZeros(pageSize);
        int pageMask = pageSize - 1;
        return numPagesFor(capacity, pageShift, pageMask);
    }

    static int numPagesFor(long capacity, int pageShift, int pageMask) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        assert numPages <= Integer.MAX_VALUE : "pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity;
        return (int) numPages;
    }

    static long capacityFor(int numPages, int pageShift) {
        return ((long) numPages) << pageShift;
    }

    public static int pageIndex(long index, int pageShift) {
        return (int) (index >>> pageShift);
    }

    public static int indexInPage(long index, int pageMask) {
        return (int) (index & pageMask);
    }

    private PageUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
