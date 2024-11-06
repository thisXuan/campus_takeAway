package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类S
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            // 2.不符合，返回信息
            return Result.fail("手机号格式错误");
        }
        // 3.生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4.存储验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.info("发送短信验证码成功，验证码：{}",code);
        // 5.发送验证码
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.校验手机号
        if(RegexUtils.isPhoneInvalid(loginForm.getPhone())){
            // 2.不符合，返回信息
            return Result.fail("手机号格式错误");
        }
        // 2.校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + loginForm.getPhone());
        if(cacheCode == null || !cacheCode.equals(loginForm.getCode())){
            // 3.不一致，返回报错
            return Result.fail("验证码错误");
        }
        // 4.一致，根据手机查询账户
        User user = query().eq("phone", loginForm.getPhone()).one();
        // 5.用户不存在，创建用户
        if(user == null){
            user = createUserWithPhone(loginForm.getPhone());
        }
        // 6.用户存在，直接登录，保存信息到redis
        // 6.1 随机生成token，做成登录令牌
        String token = UUID.randomUUID().toString(true);
        // 6.2 将user对象转化为hash存储
        UserDTO userDto = BeanUtil.copyProperties(user, UserDTO.class);
        // 6.3 存储
        Map<String, Object> userMap = BeanUtil.beanToMap(userDto,new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        String tokenKey = LOGIN_USER_KEY+token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        // 7. 返回token
        return Result.ok(token);
    }

    @Override
    public Result logout(HttpServletRequest request) {
        String token = request.getHeader("authorization");
        // 1.删除验证码的redis
        Long id = UserHolder.getUser().getId();
        User user = query().eq("id", id).one();
        String key = LOGIN_CODE_KEY+user.getPhone();
        stringRedisTemplate.delete(key);
        // 2.删除user的redis
        stringRedisTemplate.delete(LOGIN_USER_KEY+token);
        log.info("用户："+user.getNickName()+"已退出");
        return Result.ok();
    }

    private User createUserWithPhone(String phone){
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_"+RandomUtil.randomNumbers(10));
        save(user);
        return user;
    }


}
