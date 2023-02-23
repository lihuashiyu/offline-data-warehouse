package interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ETLInterceptor implements Interceptor
{
    @Override
    public void initialize() { }
    
    
    // 过滤 event 中的数据是否为 json 格式
    @Override
    public Event intercept(Event event)
    {
        byte[] body = event.getBody();                                         // 1. 获取数据
        String log = new String(body, StandardCharsets.UTF_8);
        
        if (JSONUtils.isJSONValidate(log)) { return event; }                   // 2. 校验是否为 json 
        else { return null; }
    }
    
    
    // 将处理过之后为 null 的 event 删除掉
    @Override
    public List<Event> intercept(List<Event> list)
    {
        list.removeIf(next -> intercept(next) == null);
        return list;
    }
    
    
    @Override
    public void close() { }
    
    
    public static class Builder implements Interceptor.Builder
    {
        @Override
        public Interceptor build() { return new ETLInterceptor(); }
        
        @Override
        public void configure(Context context) { }
    }
}
