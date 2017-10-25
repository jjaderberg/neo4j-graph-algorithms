package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphalgo.core.write.DisjointSetStructTranslator;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.HugeDisjointSetStructTranslator;
import org.neo4j.graphalgo.results.UnionFindResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class UnionFindProcExec {

    public static final String CONFIG_THRESHOLD = "threshold";
    public static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";


    private final GraphDatabaseAPI api;
    private final Log log;
    private final KernelTransaction transaction;
    private final String progressName;

    public static Stream<UnionFindResult> run(
            Map<String, Object> config,
            String label,
            String relationship,
            Function<ProcedureConfiguration, UnionFindProcExec> unionFind) {
        ProcedureConfiguration configuration = ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();
        UnionFindResult.Builder builder = UnionFindResult.builder();

        UnionFindProcExec uf = unionFind.apply(configuration);

        final Graph graph = uf.load(builder::timeLoad, configuration, tracker);
        DSSResult dssResult = uf.evaluate(
                builder::timeEval,
                graph,
                configuration,
                tracker);

        if (configuration.isWriteFlag()) {
            uf.write(builder::timeWrite, graph, dssResult, configuration);
        }

        return Stream.of(builder
                .withNodeCount(graph.nodeCount())
                .withSetCount(dssResult.getSetCount())
                .build());
    }

    public static Stream<DisjointSetStruct.Result> stream(
            Map<String, Object> config,
            String label,
            String relationship,
            Function<ProcedureConfiguration, UnionFindProcExec> unionFind) {
        ProcedureConfiguration configuration = ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        AllocationTracker tracker = AllocationTracker.create();
        UnionFindProcExec uf = unionFind.apply(configuration);

        final Graph graph = uf.load(configuration, tracker);
        return uf.evaluate(graph, configuration, tracker).resultStream(graph);
    }

    public static UnionFindProcExec of(
            GraphDatabaseAPI api,
            Log log,
            KernelTransaction transaction,
            String progressName,
            Function<? super Graph, ? extends GraphUnionFindAlgo<Graph, DisjointSetStruct, ?>> algo,
            BiFunction<? super HugeGraph, ? super AllocationTracker, ? extends GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, ?>> hugeAlgo) {
        return new UnionFindProcExec(api, log, transaction, progressName) {
            @Override
            protected GraphUnionFindAlgo<Graph, DisjointSetStruct, ?> algo(Graph graph) {
                return algo.apply(graph);
            }

            @Override
            protected GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, ?> hugeAlgo(HugeGraph graph, AllocationTracker tracker) {
                return hugeAlgo.apply(graph, tracker);
            }
        };
    }

    protected UnionFindProcExec(
            GraphDatabaseAPI api,
            Log log,
            KernelTransaction transaction,
            String progressName) {
        this.api = api;
        this.log = log;
        this.transaction = transaction;
        this.progressName = progressName;
    }

    public Graph load(
            Supplier<ProgressTimer> timer,
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            return load(config, tracker);
        }
    }

    public Graph load(
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        return new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(config.getNodeLabelOrQuery())
                .withOptionalRelationshipType(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        config.getProperty(),
                        config.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withAllocationTracker(tracker)
                .load(config.getGraphImpl());
    }

    public DSSResult evaluate(
            Supplier<ProgressTimer> timer,
            Graph graph,
            ProcedureConfiguration config,
            final AllocationTracker tracker) {
        try (ProgressTimer ignored = timer.get()) {
            return evaluate(graph, config, tracker);
        }
    }

    public DSSResult evaluate(Graph graph, ProcedureConfiguration config, final AllocationTracker tracker) {
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            return new DSSResult(eval(hugeAlgo(hugeGraph, tracker), config));
        }
        return new DSSResult(eval(algo(graph), config));
    }

    public void write(
            Supplier<ProgressTimer> timer,
            Graph graph,
            DSSResult struct,
            ProcedureConfiguration configuration) {
        try (ProgressTimer ignored = timer.get()) {
            write(graph, struct, configuration);
        }
    }

    public void write(Graph graph, DSSResult struct, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        Exporter exporter = Exporter.of(api, graph)
                .withLog(log)
                .parallel(
                        Pools.DEFAULT,
                        configuration.getConcurrency(),
                        TerminationFlag.wrap(transaction))
                .build();
        if (struct.hugeStruct != null) {
            write(exporter, struct.hugeStruct, configuration);
        } else {
            write(exporter, struct.struct, configuration);
        }
    }

    protected abstract GraphUnionFindAlgo<Graph, DisjointSetStruct, ?> algo(Graph graph);

    protected abstract GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, ?> hugeAlgo(HugeGraph graph, AllocationTracker tracker);

    private <G extends Graph, R> R eval(GraphUnionFindAlgo<G, R, ?> algo, ProcedureConfiguration config) {
        final R struct;
        if (config.containsKeys(ProcedureConstants.PROPERTY_PARAM, CONFIG_THRESHOLD)) {
            final Double threshold = config.get(CONFIG_THRESHOLD, 0.0);
            log.debug("Computing union find with threshold " + threshold);
            struct = algo
                    .withProgressLogger(ProgressLogger.wrap(log, progressName))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(threshold);
        } else {
            log.debug("Computing union find without threshold");
            struct = algo
                    .withProgressLogger(ProgressLogger.wrap(log, progressName))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
        }
        algo.release();
        return struct;
    }


    private void write(Exporter exporter, DisjointSetStruct struct, ProcedureConfiguration configuration) {
        exporter.write(
                configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                struct,
                DisjointSetStructTranslator.INSTANCE);
    }

    private void write(Exporter exporter, HugeDisjointSetStruct struct, ProcedureConfiguration configuration) {
        exporter.write(
                configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                struct,
                HugeDisjointSetStructTranslator.INSTANCE);
    }

}
