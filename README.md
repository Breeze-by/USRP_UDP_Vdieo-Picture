# USRP UDP 上位机发送与接收程序

这个目录现在包含两端上位机程序：

- `sender.py`：把图片或视频整理成传输比特流，按固定长度切成 UDP 帧，附带序号和 CRC32 后发送到 USRP 所连接的网口。
- `receiver.py`：从另一端 USRP 转发出来的 UDP 数据里做乱序重组、单包丢失恢复、校验与文件还原。
- `receiver.py` 现在还支持视频实时播放：接收端缓存一段连续 TS 数据后，会自动拉起 `ffplay` 开始播放。

中间 USRP-2974 如何发射和接收射频数据，这里不参与；只要求它们对网口 UDP 负载透明转发。

## 方案说明

程序没有真的把数据展开成文本形式的 `0/1`，因为 UDP 和文件读写本来就是按字节处理；这里的“bit 流”对应的是媒体文件的原始二进制序列。为了适合链路传输，程序做了这几件事：

1. 将文件字节流切成固定 `chunk_size` 的数据帧。
2. 每个帧添加会话号、分片号、分组号和 CRC32。
3. 图片直接还原原文件；视频默认尽量先转成 MPEG-TS 再发送，这样接收端能在收包过程中直接连续播放。

## 运行环境

- Python 3.12+
- Python 依赖见 `requirements.txt`
- 系统依赖：`ffmpeg`、`ffplay`

这个项目当前没有第三方 Python 包，`requirements.txt` 主要用于固定“Python 侧没有额外依赖”这件事，方便迁移时统一执行安装命令。

视频场景下，下面两个系统工具是必须的：

- `ffmpeg`：发送端把普通视频转成 MPEG-TS，接收端收完后可自动 remux 回原容器
- `ffplay`：接收端实时播放视频

也就是说：

- 只传图片：不强制需要 `ffmpeg` / `ffplay`
- 传视频但不实时播放：建议安装 `ffmpeg`
- 传视频并实时播放：必须同时安装 `ffmpeg` 和 `ffplay`

## 环境迁移

1. 安装 Python 3.12+
2. 进入项目目录
3. 安装 Python 依赖

```powershell
pip install -r .\requirements.txt
```

4. 安装 FFmpeg，并确认 `ffmpeg.exe` 和 `ffplay.exe` 都在 `PATH` 中

安装完成后建议执行：

```powershell
python --version
ffmpeg -version
ffplay -version
```

如果这三个命令都能正常输出版本信息，环境就齐了。

## 发送端

运行发送端之前，先执行一次：

```powershell
pip install -r .\requirements.txt
```

如果输入是视频，建议先确认：

```powershell
ffmpeg -version
```

发送图片：

```powershell
python .\sender.py --input ".\input.jpg" --host 192.168.10.2 --port 6000
```

发送视频：

```powershell
python .\sender.py --input ".\input.mp4" --host 192.168.10.2 --port 6000
```

常用参数：

- `--chunk-size 1316`：每个 UDP 数据帧的负载字节数，建议不超过 1400；`1316=7x188`，更适合 MPEG-TS 实时播放。
- `--packet-interval-us 200`：包间隔，链路容易拥塞时可以调大。
- `--streamable-ts` / `--no-streamable-ts`：视频是否先转成适合流式播放的 TS。

## 接收端

运行接收端之前，先执行一次：

```powershell
pip install -r .\requirements.txt
```

如果你要实时播放视频，必须先确认：

```powershell
ffplay -version
```

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 6000 --output-dir .\received
```

常用参数：

- `--once`：只接收 1 个会话，收完就退出，便于测试。
- `--idle-timeout 5`：链路静默多久后判定超时。
- `--live-play` / `--no-live-play`：是否自动实时播放视频，默认开启。
- `--playback-buffer-kb 2048`：实时播放启动前的缓存大小。大一些更稳，小一些启动更快。
- `--playback-rewind-kb 4096`：如果手动关闭播放窗口，自动重启时会回放最近这段缓存继续播。
- `--playback-no-display`：用 `ffplay` 无界面播放，适合本地自动测试或无桌面环境。
- `--auto-remux` / `--no-auto-remux`：接收结束后是否自动把 TS 还原回原容器。

### 实时播放

实时播放只对视频生效，并且要求发送端传过来的是 MPEG-TS。默认情况下：

- 发送端的 `--streamable-ts` 已经开启，会尽量把视频先转成 `.ts`
- 接收端的 `--live-play` 也已默认开启

所以典型命令就是：

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 6000 --output-dir .\received --playback-buffer-kb 2048
python .\sender.py --input ".\input.mp4" --host 192.168.10.2 --port 6000
```

如果你想更快起播，可以把缓存调小：

```powershell
python .\receiver.py --port 6000 --playback-buffer-kb 512
```

如果链路吞吐量不够，播放器会等待后续连续 TS 数据，表现出来就是卡顿。一般建议：

- 发端平均有效吞吐率高于视频平均码率
- 保留一段启动缓存，避免刚开始就因为抖动卡住
- 如果无线链路不稳，适当调大 `--packet-interval-us`

接收输出会保存在：

```text
received/session_<session_id>/
```

目录中通常包含：

- 传输过程中逐步写入的 `transport` 文件
- 收完后自动还原出的原始文件（如果启用了自动 remux 且 `ffmpeg` 可用）
- `manifest.json`

## 本地回环测试

先开接收端：

```powershell
python .\receiver.py --port 9000 --output-dir .\received --once --playback-no-display
```

再在另一个终端发送：

```powershell
python .\sender.py --input ".\input.jpg" --host 127.0.0.1 --port 9000
```

## 和 USRP 对接时的建议

- 发端上位机的 `--host` / `--port` 指向发射 USRP 上用于接收以太网 UDP 的地址。
- 收端上位机的 `receiver.py` 监听端口要和接收 USRP 输出 UDP 的目标端口一致。
- 如果无线链路误码或丢包偏多，优先调大：
  - `--packet-interval-us`
- 如果中间链路实际 MTU 偏小，减小 `--chunk-size`，避免底层 IP 分片。

## 直播说明

当前版本按“严格顺序数据”驱动实时播放：

- 接收端只会把已经连续收到的数据交给播放器。
- 如果传输慢于视频本身的数据率，播放器会卡住等待后续正确数据。
- 如果传输快于播放速率，播放器和接收端缓冲会累积，形成缓存。
- 如果手动关闭 `ffplay` 窗口，接收端会自动重启播放器并从最近缓存继续播。

发送端默认 `--chunk-size` 已调整为 `1316`，这是 `7 x 188` 字节，和 MPEG-TS 对齐，更适合实时播放。
