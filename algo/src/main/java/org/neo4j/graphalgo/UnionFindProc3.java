package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.DisjointSetStructTranslator;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.graphalgo.impl.ParallelUnionFindFJMerge;
import org.neo4j.graphalgo.results.UnionFindResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class UnionFindProc3 {

    public static final String CONFIG_THRESHOLD = "threshold";
    public static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.unionFind.exp2", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
            "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition', concurrency:4}) " +
            "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        UnionFindResult.Builder builder = UnionFindResult.builder();

        // loading
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = load(configuration);
        }

        // evaluation
        final DisjointSetStruct struct;
        try (ProgressTimer timer = builder.timeEval()) {
            struct = evaluate(graph, configuration);
        }

        if (configuration.isWriteFlag()) {
            // write back
            builder.timeWrite(() ->
                    write(graph, struct, configuration));
        }

        return Stream.of(builder
                .withNodeCount(graph.nodeCount())
                .withSetCount(struct.getSetCount())
                .build());
    }

    @Procedure(value = "algo.unionFind.exp2.stream")
    @Description("CALL algo.unionFind.stream(label:String, relationship:String, " +
            "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4}) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        // loading
        final Graph graph = load(configuration);

        // evaluation
        return evaluate(graph, configuration)
                .resultStream(graph);
    }

    private Graph load(ProcedureConfiguration config) {
        return new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(config.getNodeLabelOrQuery())
                .withOptionalRelationshipType(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        config.getProperty(),
                        config.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .load(config.getGraphImpl());
    }

    private DisjointSetStruct evaluate(Graph graph, ProcedureConfiguration config) {

        final DisjointSetStruct struct;

        if (config.getBatchSize(-1) != -1) {
            final ParallelUnionFindFJMerge parallelUnionFindFJMerge = new ParallelUnionFindFJMerge(graph, Pools.DEFAULT, config.getBatchSize(), config.getConcurrency());
            if (config.containsKeys(ProcedureConstants.PROPERTY_PARAM, CONFIG_THRESHOLD)) {
                final Double threshold = config.get(CONFIG_THRESHOLD, 0.0);
                log.debug("Computing union find with threshold in parallel" + threshold);
                struct = parallelUnionFindFJMerge
                        .withProgressLogger(ProgressLogger.wrap(log, "CC(ParallelUnionFindFJMerge)"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction))
                        .compute(threshold)
                        .getStruct();
            } else {
                log.debug("Computing union find without threshold in parallel");
                struct = parallelUnionFindFJMerge
                        .withProgressLogger(ProgressLogger.wrap(log, "CC(ParallelUnionFindFJMerge)"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction))
                        .compute()
                        .getStruct();
            }
            parallelUnionFindFJMerge.release();
        } else {
            final GraphUnionFind graphUnionFind = new GraphUnionFind(graph);
            if (config.containsKeys(ProcedureConstants.PROPERTY_PARAM, CONFIG_THRESHOLD)) {
                final Double threshold = config.get(CONFIG_THRESHOLD, 0.0);
                log.debug("Computing union find with threshold " + threshold);
                struct = graphUnionFind
                        .withProgressLogger(ProgressLogger.wrap(log, "CC(SequentialUnionFind)"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction))
                        .compute(threshold);
            } else {
                log.debug("Computing union find without threshold");
                struct = graphUnionFind
                        .withProgressLogger(ProgressLogger.wrap(log, "CC(SequentialUnionFind)"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction))
                        .compute();
            }
            graphUnionFind.release();
            graph.release();
        }

        return struct;
    }

    private void write(Graph graph, DisjointSetStruct struct, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        Exporter.of(api, graph)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.getConcurrency(), TerminationFlag.wrap(transaction))
                .build()
                .write(
                        configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                        struct,
                        DisjointSetStructTranslator.INSTANCE
                );
    }
}
