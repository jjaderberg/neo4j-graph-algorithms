package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;

import java.util.stream.Stream;

public final class DSSResult {
    public final DisjointSetStruct struct;
    public final HugeDisjointSetStruct hugeStruct;

    public DSSResult(final DisjointSetStruct struct) {
        this(struct, null);
    }

    public DSSResult(final HugeDisjointSetStruct hugeStruct) {
        this(null, hugeStruct);
    }

    private DSSResult(DisjointSetStruct struct, HugeDisjointSetStruct hugeStruct) {
        assert (struct != null && hugeStruct == null) || (struct == null && hugeStruct != null);
        this.struct = null;
        this.hugeStruct = hugeStruct;
    }

    public int getSetCount() {
        return struct != null ? struct.getSetCount() : hugeStruct.getSetCount();
    }

    public Stream<DisjointSetStruct.Result> resultStream(IdMapping idMapping) {
        return struct != null
                ? struct.resultStream(idMapping)
                : hugeStruct.resultStream(((HugeIdMapping) idMapping));
    }
}
