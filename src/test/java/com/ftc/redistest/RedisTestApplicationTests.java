package com.ftc.redistest;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.IdUtil;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RedisTestApplicationTests {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    /**
     * 测试Key
     */
    private static final String KEY = "test_key";

    @Test
    @SneakyThrows(InterruptedException.class)
    void redisLock() {

        //1.随机获取Value
        String value = IdUtil.nanoId();

        //2.Key不存在,尝试设置,过期时间2s
        Boolean success = redisTemplate.opsForValue().setIfAbsent(KEY, value, 2, TimeUnit.SECONDS);
        Assert.isTrue(Boolean.TRUE.equals(success));

        //3.Key,尝试设置,过期时间2s
        success = redisTemplate.opsForValue().setIfAbsent(KEY, value, 2, TimeUnit.SECONDS);
        Assert.isFalse(Boolean.TRUE.equals(success));

        //4.睡2s
        TimeUnit.SECONDS.sleep(2);

        //5.Key,尝试设置,过期时间2s
        success = redisTemplate.opsForValue().setIfAbsent(KEY, value, 2, TimeUnit.SECONDS);
        Assert.isTrue(Boolean.TRUE.equals(success));

        //6.尝试释放锁
        success = unlock(KEY, value);
        Assert.isTrue(Boolean.TRUE.equals(success));

        //7.再次加锁
        success = redisTemplate.opsForValue().setIfAbsent(KEY, value, 2, TimeUnit.SECONDS);
        Assert.isTrue(Boolean.TRUE.equals(success));
    }

    /**
     * 释放Redis单点模式下的分布式锁
     *
     * @param key   RedisKey
     * @param value RedisValue
     * @return 是否释放成功
     */
    private boolean unlock(String key, String value) {

        //1.读取lua脚本
        ClassPathResource unlockScript = new ClassPathResource("unlock_script.lua");

        //2.设置执行脚本以及返回类型
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(unlockScript));
        redisScript.setResultType(Long.class);

        //3.执行脚本
        Long delCount = redisTemplate.execute(redisScript, CollUtil.newArrayList(key), value);

        //4.返回结果
        return Objects.equals((long) 1, delCount);
    }
}
