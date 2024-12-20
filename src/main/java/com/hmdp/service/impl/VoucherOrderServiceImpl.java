package com.hmdp.service.impl;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RabbitTemplate rabbitTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);

    // 在当前类初始化完成后执行
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2. 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                   log.error("处理订单异常",e);
                }

            }
        }
    }

    // 运用RabbitMQ队列实现异步秒杀优化
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        // 2. 判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为0，代表没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
        // 2.2 为0，有购买资格，把下单信息保存到RabbitMQ队列
        long orderId = redisIdWorker.nextId("order");

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        String jsonStr = JSONUtil.toJsonStr(voucherOrder);
        rabbitTemplate.convertAndSend("X","XA",jsonStr);
        return Result.ok(orderId);
    }

    // 消费者代码
    @RabbitListener(queues = "QA")
    public void receiveA(Message message, Channel channel) {
        String msg = new String(message.getBody());
        log.info("正常队列:{}", msg);
        VoucherOrder voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
        save(voucherOrder);
        boolean success = seckillVoucherService.update()
                .setSql("stock= stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
    }

    @RabbitListener(queues = "QD")
    public void recieveD(Message message) {
        String msg = new String(message.getBody());
        log.info("死信队列:{}",msg);
        VoucherOrder voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
        save(voucherOrder);
        boolean success = seckillVoucherService.update()
                .setSql("stock= stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order" + voucherOrder.getUserId());

        // 获取锁
        boolean isLock = lock.tryLock();

        if(!isLock){
            // 获取互斥锁失败，返回失败信息
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        }  finally {
            // 释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;
//
//    // 用jvm阻塞队列实现异步秒杀优化
//    @Override
//    @Transactional
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        // 1. 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        // 2. 判断结果是否为0
//        int r = result.intValue();
//        if (r != 0) {
//            // 2.1 不为0，代表没有购买资格
//            return Result.fail(r==1?"库存不足":"不能重复下单");
//        }
//        // 2.2 为0，有购买资格，把下单信息保存到阻塞队列
//        long orderId = redisIdWorker.nextId("order");
//
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//
//        orderTasks.add(voucherOrder);
//        // 3. 获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 3. 返回订单id
//        return Result.ok(orderId);
//    }

//    @Override
//    @Transactional
//    public Result seckillVoucher(Long voucherId) {
//        // 1. 查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2. 判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//        // 3. 判断秒杀是否结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        // 4. 判断库存是否充足
//        if(voucher.getStock()<1){
//            return Result.fail("库存不足");
//        }
//        // 5. 扣减库存
//        // 乐观锁解决超卖问题
//        boolean success = seckillVoucherService.update().setSql("stock=stock-1").eq("voucher_id", voucherId).gt("stock",0).update();
//        if(!success){
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
//
//        //SimpleRedisLock lock = new SimpleRedisLock("order"+userId, stringRedisTemplate);
//        // 创建锁对象
//        RLock lock = redissonClient.getLock("lock:order" + userId);
//
//        // 获取锁
//        boolean isLock = lock.tryLock();
//
//        if(!isLock){
//            // 获取互斥锁失败，返回失败信息
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            // 获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }  finally {
//            // 释放锁
//            lock.unlock();
//        }
//
////        synchronized (userId.toString().intern()) {
////            // 获取代理对象
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId);
////        }
//    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = UserHolder.getUser().getId();
        // 6. 一人一单
        // 6.1 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        // 6.2 判断是否存在
        if(count>0){
            log.error("用户已经购买过一次");
            return;
        }
        boolean success = seckillVoucherService.update()
                .setSql("stock= stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            //扣减库存
            log.error("库存不足！");
            return;
        }
        save(voucherOrder);
    }
}
