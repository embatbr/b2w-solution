package steps;


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.BaseOptions;


class JsonToStringFn extends DoFn<KV<String, Iterable<JSONObject>>, String> {

    public JsonToStringFn() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        KV<String, Iterable<JSONObject>> in = context.element();
        // String out = in.getKey() + " <==> " + in.getValue().toJSONString();
        String out = in.getKey() + " <==> ";
        // String out = "";
        int count = 0;

        for(Object json : in.getValue()) {
            String dumpedJson = ((JSONObject) json).toJSONString();
            if(count == 0) {
                out = out + dumpedJson;
            }
            else {
                out = out + "," + dumpedJson;
            }

            count = count + 1;
        }

        context.output(out);
    }
}


public class LoadStep {

    private BaseOptions options;
    private PCollection<KV<String, Iterable<JSONObject>>> groupedJsons;

    public LoadStep(BaseOptions options, PCollection<KV<String, Iterable<JSONObject>>> groupedJsons) {
        this.options = options;
        this.groupedJsons = groupedJsons;
    }

    public void apply() {
        PCollection<String> lines = groupedJsons.apply(
            ParDo.of(
                new JsonToStringFn()
            )
        );

        lines.apply(
            TextIO.write()
                  .withoutSharding()
                  .to(this.options.getOutputPath())
        );
    }
}
