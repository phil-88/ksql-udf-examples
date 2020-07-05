package ru.avito.ksql.udf.udaf;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@UdafDescription(
        name = "collect_list_ext",
        description = "Gather all of the values from an input grouping into a single Array field."
)
public final class CollectListExtUdaf {

    private static final int LIMIT = 10000;

    private CollectListExtUdaf() {
        // just to make the checkstyle happy
    }

    private static <T> TableUdaf<T, List<T>, List<T>> listCollector() {
        return new TableUdaf<T, List<T>, List<T>>() {

            @Override
            public List<T> initialize() {
                return Lists.newArrayList();
                //return Lists.newArrayListWithCapacity(1000);
            }

            @Override
            public List<T> aggregate(final T thisValue, final List<T> aggregate) {
                if (aggregate.size() < LIMIT) {
                    aggregate.add(thisValue);
                }
                return aggregate;
            }

            @Override
            public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
                final int remainingCapacity = LIMIT - aggOne.size();
                aggOne.addAll(aggTwo.subList(0, Math.min(remainingCapacity, aggTwo.size())));
                return aggOne;
            }

            @Override
            public List<T> map(final List<T> agg) {
                return agg;
            }

            @Override
            public List<T> undo(final T valueToUndo, final List<T> aggregateValue) {
                aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
                return aggregateValue;
            }
        };
    }

    @UdafFactory(description = "collect values of a Bigint field into a single Array")
    public static TableUdaf<Long, List<Long>, List<Long>> createCollectListLong() {
        return listCollector();
    }

    @UdafFactory(description = "collect values of an Integer field into a single Array")
    public static TableUdaf<Integer, List<Integer>, List<Integer>> createCollectListInt() {
        return listCollector();
    }

    @UdafFactory(description = "collect values of a Double field into a single Array")
    public static TableUdaf<Double, List<Double>, List<Double>> createCollectListDouble() {
        return listCollector();
    }

    @UdafFactory(description = "collect values of a String/Varchar field into a single Array")
    public static TableUdaf<String, List<String>, List<String>> createCollectListString() {
        return listCollector();
    }

    @UdafFactory(description = "collect values of a Boolean field into a single Array")
    public static TableUdaf<Boolean, List<Boolean>, List<Boolean>> createCollectListBool() {
        return listCollector();
    }
}
