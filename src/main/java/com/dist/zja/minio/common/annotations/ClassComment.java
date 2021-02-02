package com.dist.zja.minio.common.annotations;

import java.lang.annotation.*;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-05 9:17
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ClassComment {
    String value() default "";

    //开发者
    String author() default "";
}
