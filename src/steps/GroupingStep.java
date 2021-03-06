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
            (String) in.get(this.keyName), in
        );

        context.output(out);
    }
}


class JSONArrayFn extends DoFn<KV<String, Iterable<JSONObject>>, JSONArray> {

    public JSONArrayFn() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        KV<String, Iterable<JSONObject>> in = context.element();
        JSONArray out = new JSONArray();

        for(Object obj : in.getValue()) {
            JSONObject json = (JSONObject) obj;
            out.add(json);
        }

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

    public PCollection<JSONArray> apply() {
        PCollection<KV<String, JSONObject>> keyedJsons = this.jsons.apply(
            ParDo.of(
                new JSONKeyedFn(this.options.getGroupKey())
            )
        );

        PCollection<KV<String, Iterable<JSONObject>>> keyedGroupedJsons = keyedJsons.apply(
            GroupByKey.<String, JSONObject>create()
        );

        return keyedGroupedJsons.apply(
            ParDo.of(
                new JSONArrayFn()
            )
        );
    }
}
