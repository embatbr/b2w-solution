package steps;

import java.util.ArrayList;
import java.util.Comparator;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.SortingOptions;


class SortFn extends DoFn<JSONArray, JSONArray> {

    private String keyName;

    public SortFn(String keyName) {
        super();

        this.keyName = keyName; // not used
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        JSONArray in = context.element();
        JSONArray out = (JSONArray) in.clone();

        out.sort(new Comparator<Object>() {
            @Override
            public int compare(Object A, Object B) {
                String keyA = (String) ((JSONObject) A).get("timestamp");
                String keyB = (String) ((JSONObject) B).get("timestamp");

                return keyA.compareTo(keyB);
            }
        });

        context.output(out);
    }
}


public class SortingStep {

    private SortingOptions options;
    private PCollection<JSONArray> groupedJsons;

    public SortingStep(SortingOptions options, PCollection<JSONArray> groupedJsons) {
        this.options = options;
        this.groupedJsons = groupedJsons;
    }

    public PCollection<JSONArray> apply() {
        return this.groupedJsons.apply(
            ParDo.of(
                new SortFn(this.options.getSortKey())
            )
        );
    }
}
