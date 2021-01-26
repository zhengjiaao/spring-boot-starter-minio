package com.dist.zja.minio.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 11:01
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Param {

    //参数
    String name() default "";

    //参数描述
    String description() default "";

    //参数默认值
    String defaultValue() default "";
}
