# spring-boot-starter-minio

- 封装最新的 `minio 8.0.3` 版本依赖 , 主要是简化配置、自动装配
- 适配springboot(`1.5`和`2.x`)

| spring-boot-starter-minio | springboot |
| :----: | :----: |
|  8.0 |   1.x |
|  8.0  |   2.x |


## 安装 minio

- Windwos Minio
    - 服务端(内置页面管理端)：[minio.exe](https://dl.min.io/server/minio/release/windows-amd64/minio.exe)   
    - 客户端：mc.exe  没必要安装

```cmd
启动方式：minio.exe server E:\App\minio\data

访问：http://127.0.0.1:9000
管理员：minioadmin/minioadmin

物理路径：E:\App\minio
物理数据存储路径：E:\App\minio\data
```

### 使用

引入封装依赖:
https://docs.min.io/docs/java-client-quickstart-guide

```xml
    <dependency>
        <groupId>com.dist.zja</groupId>
        <artifactId>spring-boot-starter-minio</artifactId>
        <version>8.0-SNAPSHOT</version>
    </dependency>
```

*.yaml配置

```yaml
dist:
  minio:
    config:
      enabled: true
      secure: false  # 是否启动 https 访问
      endpoint: http://127.0.0.1
      port: 9000
      accessKey: minioadmin
      secretKey: minioadmin
      default-bucket: default # 可选，仅支持小写字母,长度必须大于3个字符,默认桶会自动创建
```

bean类

```java
    //客户端操作类
    @Resource
    MinioClient minioClient;

    //桶操作类
    @Resource
    MinioBucketService minioBucketService;

    //对象操作类
    @Resource
    MinIoObjectService minIoObjectService;

    //上传文件测试方法
    @SneakyThrows
    @Override
    public String uploadFile(MultipartFile file) {
        /*ObjectWriteResponse response = minioClient.uploadObject(UploadObjectArgs.builder()
                .bucket("img")
                .object("a.jpg")  //存储对象id(文件的新名字)
                .filename("D:\\img\\a.jpg")
                .build());
        System.out.println(response.toString());*/

        //与上面一致
        return minIoObjectService.putUploadObject("img", file);
    }
```

以上完成就可以测试上传文件了!


#### 发布到公司仓库

```cmd

mvn deploy:deploy-file -DgroupId=com.dist.zja -DartifactId=spring-boot-starter-minio -Dversion=8.0-SNAPSHOT -Dpackaging=jar -Dfile=D:/project/github/private/spring-boot-starter-minio/target/spring-boot-starter-minio-8.0-SNAPSHOT.jar -Durl=http://elb-791125809.cn-northwest-1.elb.amazonaws.com.cn:5336/artifactory/libs-snapshot/ -DrepositoryId=distsnapshots -DpomFile=D:/project/github/private/spring-boot-starter-minio/pom.xml

```
