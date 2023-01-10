package com.ftc.redistest;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RedissonTests {

    @Autowired
    private RedissonClient redisson;

    /**
     * 测试Key
     */
    private static final String KEY = "test_key";

    @BeforeEach
    void unlock() {

        //1.获取锁
        RLock lock = redisson.getLock(KEY);

        //2.如果已锁定,那么释放锁
        boolean locked = lock.isLocked();
        if (locked) {
            lock.forceUnlock();
        }
    }

    @Test
    @SneakyThrows({InterruptedException.class})
    void lock() {

        //1.获取锁
        RLock lock = redisson.getLock(KEY);

        //2.判定锁是否被占有
        boolean locked = lock.isLocked();
        Assert.isFalse(locked);

        //3.上锁
        lock.lock();
        try {
            locked = lock.isLocked();
            Assert.isTrue(locked);
        } finally {

            //4.释放锁
            lock.unlock();
            locked = lock.isLocked();
            Assert.isFalse(locked);
        }

        //5.再次获取锁，并设置过期时间为5s
        lock.lock(5, TimeUnit.SECONDS);
        locked = lock.isLocked();
        Assert.isTrue(locked);

        //6.睡5s
        TimeUnit.SECONDS.sleep(5);

        //7.判定锁是否被占有
        locked = lock.isLocked();
        Assert.isFalse(locked);
    }

    @Test
    @SneakyThrows(InterruptedException.class)
    void watchDog() {

        //1.获取锁
        RLock lock = redisson.getLock(KEY);

        //2.上锁,不设置过期时间,因为看门狗线程默认续期时间为30s,因此锁默认30s过期。
        //看门狗线程每10s检测一次锁的释放情况,如果未释放锁,那么重置锁的过期时间为30s
        lock.lock();

        //3.睡10s
        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    @SneakyThrows(InterruptedException.class)
    void tryLock() {

        //1.获取锁
        RLock lock = redisson.getLock(KEY);

        //2.判定当前锁是否被占有
        boolean locked = lock.isLocked();
        Assert.isFalse(locked);

        //3.尝试获取锁,并返回获取结果
        boolean lockSuccess = lock.tryLock();
        Assert.isTrue(lockSuccess);

        //4.获取成功,进行判定并释放锁
        if (lockSuccess) {
            try {
                locked = lock.isLocked();
                Assert.isTrue(locked);
            } finally {

                //5.释放锁
                lock.unlock();
                locked = lock.isLocked();
                Assert.isFalse(locked);
            }
        }

        //6.尝试获取锁,最多阻塞10s,获取成功后,锁5s过期
        lockSuccess = lock.tryLock(10, 5, TimeUnit.SECONDS);
        Assert.isTrue(lockSuccess);

        //7.睡5s
        TimeUnit.SECONDS.sleep(5);

        //8.判定当前锁是否被占有
        locked = lock.isLocked();
        Assert.isFalse(locked);
    }

    @Test
    @SneakyThrows(InterruptedException.class)
    void lockInterruptibly() {

        //1.获取锁
        RLock lock = redisson.getLock(KEY);

        //2.创建线程1
        Thread thread1 = new Thread(() -> {
            try {
                lock.lockInterruptibly();
                System.out.println(Thread.currentThread().getName() + " get lock success");
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " lock interrupt!");
            }
        });

        //3.创建线程2
        Thread thread2 = new Thread(() -> {
            try {
                lock.lockInterruptibly();
                System.out.println(Thread.currentThread().getName() + " get lock success");
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " lock interrupt!");
            }
        });

        //4.线程1,2开始运行
        thread1.start();
        thread2.start();

        //5.线程2调用interrupt
        thread2.interrupt();

        //6.睡一会,确保子线程执行成功
        TimeUnit.SECONDS.sleep(3);
    }

    @Test
    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    void lockAsync() {

        //1.获取锁
        RLock lock = redisson.getLock(KEY);

        //2.判定锁是否获取成功
        boolean locked = lock.isLocked();
        Assert.isFalse(locked);

        //3.异步加锁,获取锁成功后,锁的有效时间为5s
        RFuture<Void> lockFuture = lock.lockAsync(5, TimeUnit.SECONDS);
        lockFuture.get();

        //4.判定锁是否获取成功
        locked = lock.isLocked();
        Assert.isTrue(locked);

        //5.异步释放锁
        RFuture<Void> unlockFuture = lock.unlockAsync();
        unlockFuture.get();

        //6.判定锁是否获取成功
        locked = lock.isLocked();
        Assert.isFalse(locked);
    }

    @Test
    void multiThread() {

        //1.创建共享变量
        List<Integer> list = CollUtil.newArrayList();

        //2.创建异步任务集合
        ArrayList<CompletableFuture<Void>> futures = CollUtil.newArrayList();

        //3.获取锁
        RLock lock = redisson.getLock(KEY);

        //4.创建10个异步任务,任务为向共享变量中添加10000条数据
        for (int i = 0; i < 10; i++) {

            //5.创建任务
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                lock.lock();
                try {
                    for (int j = 0; j < 10000; j++) {
                        list.add(j);
                    }
                } finally {
                    lock.unlock();
                }
            });

            //6.存入异步任务集合
            futures.add(future);
        }

        //7.统一管理所有异步任务
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        //8.阻塞式等待所有异步任务执行成功
        allFutures.join();

        //9.校验共享变量大小
        Assert.isTrue(100000 == list.size());
    }
}
