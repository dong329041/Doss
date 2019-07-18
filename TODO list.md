# TODO list

### 通信方式+消息序列化
目前客户端、apiServer、dataServer 之间都是通过标准的 Restful HTTP 接口通信，通信信息的序列化采用 json。下一版本改为：apiServer 对外提供 Restful 风格 HTTP 接口，而系统内部（apiServer 与 dataServer 之间）的通信方式从 HTTP + json 变成 rpc + protobuf

### 系统扩缩容优化
完善系统扩缩容时，数据迁移自平衡流程。


### 开发上层应用

1. 身份认证 IAM（单点登录、动态授权）；
2. 精细的权限控制；
3. web 界面（Angular + Nginx + go）