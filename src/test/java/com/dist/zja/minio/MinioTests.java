/**
 * @Company: 上海数慧系统技术有限公司
 * @Department: 数据中心
 * @Author: 郑家骜[ào]
 * @Email: zhengja@dist.com.cn
 * @Date: 2023-03-22 14:14
 * @Since:
 */
package com.dist.zja.minio;

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * @author: zhengja
 * @since: 2023/03/22 14:14
 */
@SpringBootApplication
@SpringBootTest
public class MinioTests {

    //桶操作类
    @Resource
    MinioBucketService minioBucketService;

    //对象操作类
    @Resource
    MinioObjectService minioObjectService;

    //客户端操作类
    @Resource
    MinioClient minioClient;

    //上传文件测试方法
    @Test
    public void test() throws Exception {
       /* ObjectWriteResponse response = minioClient.uploadObject(UploadObjectArgs.builder()
                .bucket("default")
                .object("a.jpg")  //存储对象id(文件的新名字)
                .filename("D:\\temp\\images\\a.jpg")
                .build());
        System.out.println(response.toString());*/

        //与上面一致，bucket=default
        minioObjectService.putObject("b.jpg", "D:\\temp\\images\\a.jpg");
    }

}
