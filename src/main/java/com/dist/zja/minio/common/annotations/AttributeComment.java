package com.dist.zja.minio.common.annotations;

import java.lang.annotation.*;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-05 9:18
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface AttributeComment {
    String value() default "";
}
