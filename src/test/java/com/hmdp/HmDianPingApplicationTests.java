package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker(){
        Runnable task=()->{
            for(int i=0;i<100;i++){
                long id = redisIdWorker.nextId("order");
                System.out.println(id);
            }
        };

        for(int i=0;i<300;i++){
            es.submit(task);
        }
    }

    @Test
    public void test() throws InterruptedException {
        shopService.saveShop2Redis(1L,2L);
    }


}
