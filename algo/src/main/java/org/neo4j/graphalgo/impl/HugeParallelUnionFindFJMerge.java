package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * parallel UnionFind using ExecutorService and common ForkJoin-Pool.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * Like in {@link ParallelUnionFindForkJoin} the resulting DSS of each node-partition
 * is merged by the ForkJoin pool while calculating the DSS is done by the
 * ExecutorService.
 * <p>
 * This might lead to a better distribution of tasks in the merge-tree.
 *
 * @author mknblch
 */
public class HugeParallelUnionFindFJMerge extends GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, HugeParallelUnionFindFJMerge> {

    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;
    private HugeDisjointSetStruct struct;

    public static BiFunction<HugeGraph, AllocationTracker, HugeParallelUnionFindFJMerge> of(ExecutorService executor, int minBatchSize, int concurrency) {
        return (graph, tracker) -> new HugeParallelUnionFindFJMerge(
                graph,
                executor,
                tracker,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize UF
     *
     * @param graph
     * @param executor
     */
    public HugeParallelUnionFindFJMerge(
            HugeGraph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        super(graph);
        this.executor = executor;
        this.tracker = tracker;
        nodeCount = graph.nodeCount();
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);
    }

    public HugeDisjointSetStruct compute() {

        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge(ufProcesses);
        return getStruct();
    }

    public HugeDisjointSetStruct compute(double threshold) {
        final ArrayList<TUFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return getStruct();
    }

    private void merge(ArrayList<? extends UFProcess> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        if (!running()) {
            return;
        }
        final Stack<HugeDisjointSetStruct> temp = new Stack<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct));
        struct = ForkJoinPool.commonPool().invoke(new Merge(temp));
    }

    public HugeDisjointSetStruct getStruct() {
        return struct;
    }

    @Override
    public HugeParallelUnionFindFJMerge release() {
        struct = null;
        return super.release();
    }

    /**
     * Process for finding unions of weakly connected components
     */
    private class UFProcess implements Runnable {

        protected final long offset;
        protected final long end;
        protected final HugeDisjointSetStruct struct;

        UFProcess(long offset, long length) {
            this.offset = offset;
            this.end = offset + length;
            struct = new HugeDisjointSetStruct(nodeCount, tracker).reset();
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                try {
                    graph.forEachRelationship(
                            node,
                            Direction.OUTGOING,
                            (sourceNodeId, targetNodeId) -> {
                                if (!struct.connected(
                                        sourceNodeId,
                                        targetNodeId)) {
                                    struct.union(sourceNodeId, targetNodeId);
                                }
                                return true;
                            });
                } catch (Exception e) {
                    System.out.println("exception for nodeid:" + node);
                    e.printStackTrace();
                    return;
                }
            }
            getProgressLogger().logProgress((end - 1) / (nodeCount - 1));
        }
    }

    /**
     * Process to calc a DSS using a threshold
     */
    private class TUFProcess extends UFProcess {

        private final double threshold;

        TUFProcess(long offset, long length, double threshold) {
            super(offset, length);
            this.threshold = threshold;
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            double weight = graph.weightOf(
                                    sourceNodeId,
                                    targetNodeId);
                            if (weight > threshold) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
        }
    }

    private class Merge extends RecursiveTask<HugeDisjointSetStruct> {

        private final Stack<HugeDisjointSetStruct> structs;

        private Merge(Stack<HugeDisjointSetStruct> structs) {
            this.structs = structs;
        }

        @Override
        protected HugeDisjointSetStruct compute() {
            final int size = structs.size();
            if (size == 1) {
                return structs.pop();
            }
            if (!running()) {
                return structs.pop();
            }
            if (size == 2) {
                return merge(structs.pop(), structs.pop());
            }
            final Stack<HugeDisjointSetStruct> list = new Stack<>();
            list.push(structs.pop());
            list.push(structs.pop());
            final Merge mergeA = new Merge(structs);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final HugeDisjointSetStruct computed = mergeB.compute();
            return merge(mergeA.join(), computed);
        }

        private HugeDisjointSetStruct merge(
                HugeDisjointSetStruct a,
                HugeDisjointSetStruct b) {
            return a.merge(b);
        }
    }


}
