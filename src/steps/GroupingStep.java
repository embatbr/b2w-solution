package steps;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.GroupingOptions;


class JSONKeyedFn extends DoFn<JSONObject, KV<String, JSONObject>> {

    private String keyName;

    public JSONKeyedFn(String keyName) {
        super();

        this.keyName = keyName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        JSONObject in = context.element();
        KV<String, JSONObject> out = KV.of(
            // (String) in.get("customer"), in
            (String) in.get(this.keyName), in
        );

        context.output(out);
    }
}


public class GroupingStep {

    private GroupingOptions options;
    private PCollection<JSONObject> jsons;

    public GroupingStep(GroupingOptions options, PCollection<JSONObject> jsons) {
        this.options = options;
        this.jsons = jsons;
    }

    public PCollection<KV<String, Iterable<JSONObject>>> apply() {
        PCollection<KV<String, JSONObject>> keyedJsons = this.jsons.apply(
            ParDo.of(
                // new JSONKeyedFn()
                new JSONKeyedFn(this.options.getGroupKey())
            )
        );

        PCollection<KV<String, Iterable<JSONObject>>> groupedJsons = keyedJsons.apply(
            GroupByKey.<String, JSONObject>create()
        );

        return groupedJsons;
    }
}
