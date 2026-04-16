# USRP UDP 共享目录传输

这个项目现在用于在两台电脑之间通过 USRP 透明转发的 UDP 链路同步一个共享目录。

发送端运行 `sender.py`，持续扫描 `shared/`（或你指定的目录），把其中的目录项按相对路径顺序逐个封装成 UDP 会话发送出去。
接收端运行 `receiver.py`，按收到的相对路径把文件和目录恢复到输出目录中。

当前设计假设：

- LTE / USRP 链路不会把错误比特上传给上层。
- 上层需要处理的是丢包，不是比特翻转。
- 如果某个文件会话缺包，接收端会报告丢失并丢弃这个文件的临时副本，然后继续接收后续文件。
- 如果某个文件后续再次更新，发送端会把新的稳定版本重新发送，接收端会覆盖恢复为最新版。

## 当前行为

- 递归发送整个目录树中的文件和目录。
- 文件按相对路径恢复，内容按原始字节恢复。
- 文件只在接收完整且 SHA256 校验通过后才替换正式文件。
- 目录会被单独同步，因此空目录也能恢复。
- 发送端默认持续轮询目录，发现新增或更新的稳定文件后立即发送。
- 接收端默认持续运行，可连续接收多个文件会话。
- 如果某个会话收到了 `END` 但仍缺块，接收端会在短暂等待后报告缺包并跳过该文件。

## 环境安装

当前版本仅依赖 Python 标准库。

```powershell
pip install -r .\requirements.txt
```

如果你想单独用项目虚拟环境：

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1
```

## 发送端用法

持续扫描 `shared/` 并发送：

```powershell
python .\sender.py --input-dir .\shared --host 169.254.46.237 --port 50000
```

只发送当前目录快照，发完退出：

```powershell
python .\sender.py --input-dir .\shared --host 169.254.46.237 --port 50000 --scan-once
```

常用参数：

- `--input-dir .\shared`：要递归发送的目录。
- `--chunk-size 1316`：每个 UDP 数据包的有效载荷大小。
- `--target-rate-mbps 8`：按目标有效载荷速率发包，单位是 `MB/s`。
- `--packet-interval-us 200`：固定包间隔；设置了 `--target-rate-mbps` 后忽略。
- `--scan-interval-ms 500`：目录轮询间隔。
- `--settle-ms 500`：文件在这段时间内保持不变后才会被发送，用于避开发送正在写入中的中间态。
- `--scan-once`：只发送当前稳定快照，不持续 watch。

发送端日志会显示：

- 当前发送的相对路径
- 会话序号和会话 ID
- 文件大小和 chunk 数
- 当前吞吐率
- 退出时的总条目数、总字节数、总包数

## 接收端用法

持续接收并恢复到 `received/`：

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 60000 --output-dir .\received
```

本机测试时，接收完一段传输后如果全局空闲 2 秒就退出：

```powershell
python .\receiver.py --bind-host 127.0.0.1 --port 9000 --output-dir .\received --exit-when-idle 2
```

常用参数：

- `--output-dir .\received`：恢复后的目录根路径。
- `--idle-timeout 5`：某个会话长时间没有后续数据时，按失败处理。
- `--post-end-timeout-ms 1000`：已经收到 `END` 后，如果仍缺块，等待多久后报告缺包并跳过该文件。
- `--packet-queue-size 8192`：用户态收包队列长度。
- `--exit-when-idle 2`：测试辅助参数。看到至少一个包后，如果没有活跃会话并且全局空闲达到设定秒数，就退出。

接收端日志会显示：

- 新会话的相对路径和序号
- 完成恢复的文件路径
- 未完成文件的缺块数和丢弃原因
- 活跃会话的接收进度

## 丢包处理

当前版本的丢包策略是：

1. 已收到的 chunk 直接按偏移写入临时文件。
2. 如果所有 chunk 都收到并且 SHA256 校验通过，就原子替换正式文件。
3. 如果在 `END` 之后仍缺 chunk，或者会话超时，则打印丢包报告。
4. 失败文件的临时副本会被删除，不会污染输出目录。
5. 后续文件会继续正常接收和恢复。

这对应你当前的要求：先报告并跳过坏文件，后续更复杂的丢包处理策略可以后面再接着加。

## 本地回环测试

静态目录快照测试：

```powershell
python .\receiver.py --bind-host 127.0.0.1 --port 9100 --output-dir .\tmp_out --exit-when-idle 1
python .\sender.py --input-dir .\shared --host 127.0.0.1 --port 9100 --scan-once --settle-ms 0
```

持续 watch 测试：

```powershell
python .\receiver.py --bind-host 127.0.0.1 --port 9100 --output-dir .\tmp_out --exit-when-idle 4
python .\sender.py --input-dir .\shared --host 127.0.0.1 --port 9100 --scan-interval-ms 200 --settle-ms 200
```

## 与 USRP 对接

- `sender.py --host --port` 指向发射侧 USRP 的 UDP 输入地址。
- `receiver.py --port` 对应接收侧 USRP 的 UDP 输出端口。
- 这套上位机逻辑不参与 LTE 侧纠错。
- 如果链路没有干扰且不丢包，接收端恢复出的文件与发送端原文件一致。
- 如果链路丢包，接收端会报告该文件缺失的 chunk 数并跳过该文件，继续恢复后续文件。
