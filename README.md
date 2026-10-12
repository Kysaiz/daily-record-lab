# daily-record-lab

用于观察GMS公开排行榜数据的小规模实验，仅手动启动。

## 测试范围

使用一个标准Ubuntu runner、一个HTTPS连接，整体速率为每秒0.4次请求。读取Bishop职业榜在两种服务器类型合并范围内的前100个非重叠页面，随后复查首尾两个页面。最多发送102次GET请求。

该测试只覆盖一个小分片，不代表整个Bishop职业或全部LV275+角色已采集完成。

## 停止条件

出现HTTP拒绝、网络错误、空页或不足10条的页面、职业不符、角色重复、totalCount变化或首尾校验变化时，立即结束。不重试，不变换身份，不自动安排后续运行。

## 如何运行

打开仓库的Actions，选择Single-runner integrity observation，点击Run workflow。直接运行probe_job_pages.py但不加--execute时，只打印测试计划，不发送请求。

## 如何验收

下载single-runner-observation结果包，检查report.json：

- status为complete_sample。
- 成功扫描100页，取得1000个不同的角色。
- duplicate_rows为0，首尾校验一致。

任务显示绿色只表示本次执行完成，不能证明全量采集可靠或长期不会限流。

## 数据与限制

结果包保留3天，包含官方公开角色记录、图片URL和逐请求时间及状态，不下载图片文件。日志中的socket本地地址可能是云环境内网地址，不是测得的公网出口IP。

无需提供账号密码、Nexon凭据或NAS数据库。本实验不能单独确定官方限流规则，也不证明每日全量方案已经成立。
