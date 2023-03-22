# spring-boot-starter-minio

- 封装最新的 `minio 8.5.2` 版本依赖 , 主要是简化配置、自动装配
- 适配springboot(`1.5`和`2.x`)

| spring-boot-starter-minio | minio server | springboot |
| :----: | :----: | :----: | 
|  8.5.2 |  [RELEASE.2023-03-20T20-16-18Z](https://github.com/minio/minio/releases/tag/RELEASE.2023-03-20T20-16-18Z)  | 1.x | 
|  8.5.2  | [RELEASE.2023-03-20T20-16-18Z](https://github.com/minio/minio/releases/tag/RELEASE.2023-03-20T20-16-18Z)  | 2.x | 


## 安装 minio

- [minio server 版本列表](https://dl.min.io/server/minio/release/)
- Windwos Minio
    - 服务端(内置页面管理端)：[minio.exe](https://dl.min.io/server/minio/release/windows-amd64/minio.exe)   
    - 客户端：mc.exe  没必要安装

### 使用

引入封装依赖:

```xml
    <dependency>
        <groupId>com.dist.zja</groupId>
        <artifactId>spring-boot-starter-minio</artifactId>
        <version>8.5.2</version>
    </dependency>
```

*.yaml配置

```yaml
dist:
  minio:
    enabled: true  # 默认true
    secure: false  # 是否启动 https 访问, 默认 false
    endpoint: http://127.0.0.1
    port: 9000
    accessKey: admin
    secretKey: password
    default-bucket: default # 可选，仅支持小写字母,长度必须大于3个字符,默认桶会自动创建

```

bean类

```java
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
```

以上完成就可以测试上传文件了!
