package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * parallel UnionFind using ExecutorService only.
 * <p>
 * Algorithm based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The implementation is based on a queue which acts as a buffer
 * for each computed DSS. As long as there are more elements on
 * the queue the algorithm takes two, merges them and adds its
 * result to the queue until only 1 element remains.
 *
 * @author mknblch
 */
public class HugeParallelUnionFindQueue extends Algorithm<HugeParallelUnionFindQueue> {

    private HugeGraph graph;
    private final ExecutorService executor;
    private final long nodeCount;
    private final long batchSize;
    private final AllocationTracker tracker;
    private final BlockingQueue<HugeDisjointSetStruct> queue;
    private final List<Future<?>> futures;

    /**
     * initialize parallel UF
     */
    public HugeParallelUnionFindQueue(
            HugeGraph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            AllocationTracker tracker) {
        this.graph = graph;
        this.executor = executor;
        nodeCount = graph.nodeCount();
        this.tracker = tracker;
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize,
                Integer.MAX_VALUE);

        long targetSteps = Math.floorDiv(nodeCount, batchSize) - 1;
        if (targetSteps > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "too many nodes (%d) to run union find with the given concurrency (%d) and batchSize (%d)",
                    nodeCount,
                    concurrency,
                    batchSize));
        }

        queue = new LinkedBlockingQueue<>();
        futures = new ArrayList<>();
    }

    public HugeParallelUnionFindQueue compute() {
        final int steps = (int) (Math.floorDiv(nodeCount, batchSize) - 1);
        for (long i = 0L; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new HugeUnionFindTask(i)));
        }
        for (int i = steps - 1; i >= 0; i--) {
            futures.add(executor.submit(() -> {
                final HugeDisjointSetStruct a;
                final HugeDisjointSetStruct b;
                try {
                    a = queue.take();
                    b = queue.take();
                    queue.add(a.merge(b));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));
        }

        await();

        return this;
    }

    private void await() {
        ParallelUtil.awaitTermination(futures);
    }

    public HugeParallelUnionFindQueue compute(double threshold) {
        throw new IllegalArgumentException("Not yet implemented");
    }

    public HugeDisjointSetStruct getStruct() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public HugeParallelUnionFindQueue me() {
        return this;
    }

    @Override
    public HugeParallelUnionFindQueue release() {
        graph = null;
        return null;
    }

    private class HugeUnionFindTask implements Runnable {

        protected final long offset;
        protected final long end;

        public HugeUnionFindTask(long offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        public void run() {
            final HugeDisjointSetStruct struct = new HugeDisjointSetStruct(nodeCount, tracker).reset();
            for (long node = offset; node < end; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                    if (!struct.connected(sourceNodeId, targetNodeId)) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
            }
            getProgressLogger().logProgress((end - 1.0) / (nodeCount - 1.0));
            queue.add(struct);
        }
    }
}
