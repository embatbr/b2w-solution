package steps;

import java.time.Duration;
import java.time.LocalDateTime;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.GroupingAndSortingOptions;


class SectionClassifierFn extends DoFn<JSONArray, JSONArray> {

    public SectionClassifierFn() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        JSONArray in = context.element();
        JSONArray out = new JSONArray();

        boolean logged = true;

        for(int i = 1; i < in.size(); i++) {
            JSONObject cur_json = (JSONObject) in.get(i);
            JSONObject prev_json = (JSONObject) in.get(i - 1);

            String prev_page = (String) prev_json.get("page");
            if(prev_page.equals("basket")) {
                String cur_dt_str = ((String) cur_json.get("timestamp")).replace(' ', 'T');
                LocalDateTime cur_dt = LocalDateTime.parse(cur_dt_str);
                String prev_dt_str = ((String) prev_json.get("timestamp")).replace(' ', 'T');
                LocalDateTime prev_dt = LocalDateTime.parse(prev_dt_str);

                Duration duration = Duration.between(prev_dt, cur_dt);
                long total_milliseconds = duration.toMillis();
                if(total_milliseconds > (600*1000)) {
                    out.add((JSONObject) prev_json.clone());
                    logged = false;
                    break;
                }
            }
        }

        if(logged) {
            int last_index = in.size() - 1;
            JSONObject last_json = (JSONObject) in.get(last_index);
            String last_page = (String) last_json.get("page");
            if(last_page.equals("basket")) {
                out.add((JSONObject) last_json.clone());
            }
        }

        // JSONObject prev_json = null;
        // LocalDateTime prev_dt = null;
        // LocalDateTime cur_dt = null;

        // for(Object obj : in) {
        //     JSONObject json = (JSONObject) obj;
        //     String ts_str = (String) json.get("timestamp");
        //     String ts_str_iso = ts_str.replace(' ', 'T');
        //     String page_type = (String) json.get("page");

        //     cur_dt = LocalDateTime.parse(ts_str_iso);
        //     if(prev_dt != null) {
        //         Duration duration = Duration.between(prev_dt, cur_dt);
        //         long total_milliseconds = duration.toMillis();
        //         if(total_milliseconds > (600*1000)) {
        //             // out.add((JSONObject) json.clone());
        //         }

        //         JSONObject json_out = new JSONObject();
        //         json_out.put("duration", total_milliseconds);
        //         out.add(json_out);
        //     }

        //     // JSONObject json_clone = (JSONObject) json.clone();
        //     // json_clone.put("timestamp", ts_str_iso);

        //     prev_json = json;
        //     prev_dt = cur_dt;
        // }

        context.output(out);
    }
}


public class ClassificationStep {

    private GroupingAndSortingOptions options;
    private PCollection<JSONArray> sortedGroupedJsons;

    public ClassificationStep(GroupingAndSortingOptions options, PCollection<JSONArray> sortedGroupedJsons) {
        this.options = options;
        this.sortedGroupedJsons = sortedGroupedJsons;
    }

    public PCollection<JSONArray> apply() {
        return this.sortedGroupedJsons.apply(
            ParDo.of(
                new SectionClassifierFn()
            )
        );
    }
}
