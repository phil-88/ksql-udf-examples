package ru.avito.ksql.udf.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.*;

@UdfDescription(name = "outlier_percentile", description = "calculates outliers based on specified percentile")
public class OutlierPercentile {

    @Udf(description = "outliers for map")
    public Map<String, Long>
    outlier_percentile(@UdfParameter("map") final Map<String, Long> histogram,
                       @UdfParameter("pcnt") int percent,
                       @UdfParameter("factor") double factor,
                       @UdfParameter("min") long min) {

        int total = histogram.size();
        if (total <= 1) {
            return null;
        }

        // build reversed index
        ArrayList<Long> histAsList = new ArrayList<>(total);
        TreeMap<Long, LinkedList<String>> reversed = new TreeMap<>();

        for (Map.Entry<String, Long> entry : histogram.entrySet()) {
            Long val = entry.getValue();
            if (val > min) {
                reversed.computeIfAbsent(val, k -> new LinkedList<>()).add(entry.getKey());
            }
            histAsList.add(val);
        }

        if (reversed.isEmpty()) {
            return null;
        }

        // calculate threshold
        int thrInd = (int) (percent * 0.01 * total);
        histAsList.sort(Long::compareTo);
        Long thr = histAsList.get(Math.max(0, Math.min(thrInd, histAsList.size() - 1)));

        if (thr * factor > histAsList.get(histAsList.size() - 1)) {
            return null;
        }

        // find outliers
        Map<String, Long> res = new HashMap<>();
        for (Map.Entry<Long, LinkedList<String>> entry : reversed.entrySet()) {
            if (entry.getKey() > thr * factor) {
                for (String cohort : entry.getValue()) {
                    res.put(cohort, entry.getKey());
                }
            }
        }
        return res.isEmpty() ? null : res;
    }


    @Udf(description = "outliers for list")
    public Map<String, Long>
    outlier_percentile(@UdfParameter("map") final List<Integer> serie,
                       @UdfParameter("pcnt") int percent,
                       @UdfParameter("factor") double factor,
                       @UdfParameter("min") long min) {

        if (serie.size() <= 1 || serie.size() < min) {
            return null;
        }

        // build reversed index
        ArrayList<Long> histAsList = new ArrayList<>(serie.size());
        TreeMap<Long, LinkedList<Integer>> reversed = new TreeMap<>();

        serie.sort(Integer::compareTo);

        Integer key = serie.get(0);
        long val = 0;
        for (Integer i : serie) {
            if (i.equals(key)) {
                val += 1;
            } else {
                if (val > min) {
                    reversed.computeIfAbsent(val, k -> new LinkedList<>()).add(key);
                }
                histAsList.add(val);
                key = i;
                val = 1;
            };
        }
        if (val > min) {
            reversed.computeIfAbsent(val, k -> new LinkedList<>()).add(key);
        }
        histAsList.add(val);

        if (reversed.isEmpty()) {
            return null;
        }

        // calculate threshold
        int thrInd = (int) (percent * 0.01 * histAsList.size());
        histAsList.sort(Long::compareTo);
        Long thr = histAsList.get(Math.max(0, Math.min(thrInd, histAsList.size() - 1)));

        if (thr * factor > histAsList.get(histAsList.size() - 1)) {
            return null;
        }

        // find outliers
        Map<String, Long> res = new HashMap<>();
        for (Map.Entry<Long, LinkedList<Integer>> entry : reversed.entrySet()) {
            if (entry.getKey() > thr * factor) {
                for (Integer cohort : entry.getValue()) {
                    res.put(String.valueOf(cohort), entry.getKey());
                }
            }
        }
        return res.isEmpty() ? null : res;
    }

}
