package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.BiFunction;

/**
 * parallel UnionFind using common ForkJoin-Pool only.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The UnionFindTask extracts a nodePartition if its input-set is too big and
 * calculates its result while lending the rest-nodeSet to another FJ-Task.
 * <p>
 * Note: The splitting method might be sub-optimal since the resulting work-tree is
 * very unbalanced so each thread needs to wait for its predecessor to complete
 * before merging his set into the parent set.
 *
 * @author mknblch
 */
public class HugeParallelUnionFindForkJoin extends GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, HugeParallelUnionFindForkJoin> {

    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;

    public static BiFunction<HugeGraph, AllocationTracker, HugeParallelUnionFindForkJoin> of(
            int minBatchSize,
            int concurrency) {
        return (graph, tracker) -> new HugeParallelUnionFindForkJoin(
                graph,
                tracker,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize parallel UF
     *
     * @param graph
     */
    private HugeParallelUnionFindForkJoin(
            HugeGraph graph,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        super(graph);
        nodeCount = graph.nodeCount();
        this.tracker = tracker;
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);

    }

    public HugeDisjointSetStruct compute() {
        return ForkJoinPool.commonPool().invoke(new UnionFindTask(0));
    }


    public HugeDisjointSetStruct compute(double threshold) {
        return ForkJoinPool
                .commonPool()
                .invoke(new ThresholdUFTask(0, threshold));
    }

    private class UnionFindTask extends RecursiveTask<HugeDisjointSetStruct> {

        protected final long offset;
        protected final long end;

        UnionFindTask(long offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        protected HugeDisjointSetStruct compute() {
            if (nodeCount - end >= batchSize && running()) {
                final UnionFindTask process = new UnionFindTask(end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected HugeDisjointSetStruct run() {
            final HugeDisjointSetStruct struct = new HugeDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            if (!struct.connected(sourceNodeId, targetNodeId)) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
            getProgressLogger().logProgress(end - 1, nodeCount - 1);

            return struct;
        }
    }

    private class ThresholdUFTask extends UnionFindTask {

        private final double threshold;

        ThresholdUFTask(long offset, double threshold) {
            super(offset);
            this.threshold = threshold;
        }

        protected HugeDisjointSetStruct run() {
            final HugeDisjointSetStruct struct = new HugeDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (source, target) -> {
                            double weight = graph.weightOf(source, target);
                            if (weight >= threshold && !struct.connected(
                                    source,
                                    target)) {
                                struct.union(source, target);
                            }
                            return true;
                        });
            }
            return struct;
        }
    }
}
