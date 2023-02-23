package interceptor;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

public class JSONUtils
{
    // 验证数据是否为 json
    public static boolean isJSONValidate(String log)
    {
        try
        {
            JSONObject.parse(log);
            return true;
        } catch (JSONException e) { return false; }
    }
}
