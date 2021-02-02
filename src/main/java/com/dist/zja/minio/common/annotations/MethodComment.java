package com.dist.zja.minio.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 10:47
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface MethodComment {

    //功能
    String function() default "";

    //参数
    Param[] params() default {};

    //描述
    String description() default "";

    //开发者
    String author() default "";

}
