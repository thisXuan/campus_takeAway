package com.hmdp.service.impl;

import cn.hutool.cache.Cache;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.apache.tomcat.jni.Local;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryShopById(Long id) {
        // Shop shop = cacheClient.queryWithPathThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_NULL_TTL,TimeUnit.MINUTES);
        Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_NULL_TTL, TimeUnit.MINUTES);
        // Shop shop = queryWithPathThrough(id);
        // Shop shop = queryWithMutex(id);
        // Shop shop = queryWithLogicExpire(id);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    // 运用互斥锁解决缓存雪崩
    public Shop queryWithMutex(Long id){
        // 1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.存在，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if(shopJson!=null){
            return null;
        }
        // 3.实现缓存重建
        // 3.1 创建互斥锁
        String lockKey = LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            if(!tryLock(lockKey)){
                // 3.2 创建失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 3.3 创建成功，根据id查询数据库
            shop = getById(id);
            // 模拟重建的延时
            Thread.sleep(200);
            // 4.数据库不存在，直接返回
            if(shop==null){
                // 将空值写入redis, 运用空值解决缓存穿透
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            // 5.数据库存在，将数据写入redis中
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6.释放互斥锁
            unlock(lockKey);
        }
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 运用逻辑过期解决缓存雪崩，需要提前加入热点key进行缓存预热
    public Shop queryWithLogicExpire(Long id){
        // 1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.不存在，直接返回
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        RedisData redisdata = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisdata.getData();
        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
        LocalDateTime expiredTime = redisdata.getExpireTime();
        // 3.存在，判断缓存是否过期
        if(expiredTime.isAfter(LocalDateTime.now())){
            // 3.1 未过期，直接返回
            return shop;
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
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        // 4.4 失败，直接 返回
        return shop;
    }


    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expiredSeconds) throws InterruptedException {
        Shop shop = getById(id);
        Thread.sleep(10);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expiredSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    // 缓存穿透（空值法）
    public Shop queryWithPathThrough(Long id) {
        // 1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.存在，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if(shopJson!=null){
            return null;
        }
        // 3.不存在，访问数据库查询
        Shop shop = getById(id);
        // 4.数据库不存在，直接返回
        if(shop==null){
            // 将空值写入redis, 运用空值解决缓存穿透
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 5.数据库存在，将数据写入redis中
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
