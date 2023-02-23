package interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimeInterceptor implements Interceptor
{
    @Override
    public void initialize() { }
    
    
    // 将 Event 中的数据里面的时间戳取出来，放入 headers，提供给 HDFS 的 Sink 使用，用作控制输出文件路径
    @Override
    public Event intercept(Event event)
    {
        // 1 获取 body 中的数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        
        // 2 将 json 数据转为 对象
        JSONObject jsonObject = JSONObject.parseObject(log);
        Long ts = jsonObject.getLong("ts");
        
        // 3 将 timestamp 赋值给 ts 
        Map<String, String> headers = event.getHeaders();
        headers.put("timestamp", String.valueOf(ts * 1000));
        
        return event;
    }
    
    
    @Override
    public List<Event> intercept(List<Event> eventList)
    {
        for (Event event : eventList) { intercept(event); }
        return eventList;
    }
    
    
    @Override
    public void close() { }
    
    
    public static class Builder implements Interceptor.Builder
    {
        @Override
        public Interceptor build() { return new TimeInterceptor(); }
        
        @Override
        public void configure(Context context) { }
    }
}
