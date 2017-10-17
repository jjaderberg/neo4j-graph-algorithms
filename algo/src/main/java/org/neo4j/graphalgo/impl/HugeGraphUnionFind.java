package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * Sequential UnionFind:
 * <p>
 * The algorithm computes sets of weakly connected components.
 * <p>
 * The impl. is based on the {@link DisjointSetStruct}. It iterates over all relationships once
 * within a single forEach loop and adds each source-target pair to the struct. Therefore buffering
 * would introduce an overhead (a SingleRun-RelationshipIterable might be used here).
 * <p>
 * There are 2 different methods for computing the component-sets. compute() calculates all weakly
 * components regardless of the actual weight of the relationship while compute(threshold:double)
 * on the other hand only takes the transition into account if the weight exceeds the threshold value.
 *
 * @author mknblch
 */
public class HugeGraphUnionFind extends Algorithm<HugeGraphUnionFind> {

    private HugeGraph graph;

    private HugeDisjointSetStruct dss;
    private final long nodeCount;

    public HugeGraphUnionFind(
            HugeGraph graph,
            AllocationTracker tracker) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.dss = new HugeDisjointSetStruct(nodeCount, tracker);
    }

    /**
     * compute unions of connected nodes
     *
     * @return a DSS
     */
    public HugeDisjointSetStruct compute() {
        dss.reset();
        final ProgressLogger progressLogger = getProgressLogger();
        graph.forEachNode((long node) -> {
            if (!running()) {
                return false;
            }
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target) -> {
                dss.union(source, target);
                return true;
            });
            progressLogger.logProgress((double) node / (nodeCount - 1));
            return true;
        });
        return dss;
    }

    /**
     * compute unions if relationship weight exceeds threshold
     *
     * @param threshold the minimum threshold
     * @return a DSS
     */
    public HugeDisjointSetStruct compute(final double threshold) {
        dss.reset();
        final ProgressLogger progressLogger = getProgressLogger();
        graph.forEachNode((long node) -> {
            if (!running()) {
                return false;
            }
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target) -> {
                double weight = graph.weightOf(source, target);
                if (weight >= threshold) {
                    dss.union(source, target);
                }
                return true;
            });
            progressLogger.logProgress((double) node / (nodeCount - 1));
            return true;
        });
        return dss;
    }

    @Override
    public HugeGraphUnionFind me() {
        return this;
    }

    @Override
    public HugeGraphUnionFind release() {
        graph = null;
        dss = null;
        return this;
    }
}
