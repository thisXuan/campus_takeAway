package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
// 封装Redis工具类
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        String jsonStr = JSONUtil.toJsonStr(value);
        stringRedisTemplate.opsForValue().set(key, jsonStr, time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置RedisData对象
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPathThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time,TimeUnit unit) {
        String key = keyPrefix+id;
        // 1.从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.存在，直接返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        if(json!=null){
            return null;
        }
        // 3.不存在，访问数据库查询
        R r = dbFallback.apply(id);
        // 4.数据库不存在，直接返回
        if(r==null){
            // 将空值写入redis, 运用空值解决缓存穿透
            stringRedisTemplate.opsForValue().set(key,"",time,unit);
            return null;
        }
        // 5.数据库存在，将数据写入redis中
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),time, unit);
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R,ID> R queryWithLogicExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix+id;
        // 1.从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.不存在，直接返回
        if(StrUtil.isBlank(json)){
            return null;
        }
        RedisData redisdata = JSONUtil.toBean(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisdata.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        LocalDateTime expiredTime = redisdata.getExpireTime();
        // 3.存在，判断缓存是否过期
        if(expiredTime.isAfter(LocalDateTime.now())){
            // 3.1 未过期，直接返回
            return r;
        }
        // 3.2 已过期，缓存重建
        // 4. 缓存重建
        // 4.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 4.2 获取互斥锁是否成功
        if(isLock){
            // 4.3 成功，开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }

            });
        }
        // 4.4 失败，直接 返回
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
