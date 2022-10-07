package schema;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class KafkaSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建json
        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        System.out.println("sourceRecord:"+sourceRecord);

        //
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
//Struct{id=1122553,name=cpu.usage,hostname=svr1002,value=99999,timestamp=1655558465000}

        //before数据
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeJson.put(field.name(), before.get(field));
            }
            beforeJson.put("timenew",System.currentTimeMillis());

        }
        result.put("before", beforeJson);

        JSONObject afterJson = new JSONObject();
        //after数据
        if (after != null) {
            //获取列信息
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterJson.put(field.name(), after.get(field));
            }
            afterJson.put("timenew",System.currentTimeMillis());

        }
        result.put("after", afterJson);

        System.out.println("after:"+afterJson);
        System.out.println("before:"+beforeJson);
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);



        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
